import math
from collections import deque
from collections.abc import Mapping

_SNAPSHOT_FIELDS = frozenset(('values', 'events', 'seen', 'max_queue'))
# 可选字段：event_expiries（事件 TTL 对齐信息）、overflow_policy（溢出策略）、
# discard_history（丢弃审计历史）与 discard_history_limit（历史容量）、
# event_receipts（与事件对齐的回执）与 next_receipt（下一个待分配回执）、
# event_metadata（与事件对齐的去重元数据）
_SNAPSHOT_FIELDS_ALL = _SNAPSHOT_FIELDS | frozenset((
    'event_expiries', 'overflow_policy', 'discard_history', 'discard_history_limit',
    'event_receipts', 'next_receipt', 'clock_policy', 'last_clock_time',
    'event_metadata'))

_OVERFLOW_POLICIES = frozenset(('reject_new', 'drop_oldest'))

# 实时单调时钟保护策略：allow_regression 沿用允许时钟回退的既有行为；
# reject_regression 在每次实际采样时拒绝小于最近成功采样水位的读数
_CLOCK_POLICIES = frozenset(('allow_regression', 'reject_regression'))


class ClockRegressionError(RuntimeError):
    """reject_regression 策略下注入时钟读数小于最近成功采样水位时抛出。

    抛出时调用尚未改变任何状态：值、事件、去重表、回执、丢弃历史与时间
    水位全部保持调用前原样。current 为本次被拒绝的读数，last_time 为最近
    一次成功采样的水位（首次采样前不会抛出本异常）。
    """

    def __init__(self, current, last_time):
        super().__init__(
            'clock regressed: observed %r, last successful sample was %r'
            % (current, last_time))
        self.current = current
        self.last_time = last_time

# 丢弃审计历史只记录两种原因：事件 TTL 到期清理与 drop_oldest 队首挤出
_DISCARD_REASONS = frozenset(('event_ttl', 'queue_full'))
_DISCARD_ENTRY_FIELDS = frozenset(('event', 'reason', 'timestamp'))

# 队列条目去重元数据的三个字段：dedupe_known 标记去重键是否已知（区分真实
# 的 None 键与未知键），dedupe_key 为入队时的去重键，dedupe_expires_at 为
# 接收时刻加去重窗口的绝对到期点；未知键条目后两个字段恒为 None
_METADATA_ENTRY_FIELDS = frozenset(
    ('dedupe_known', 'dedupe_key', 'dedupe_expires_at'))

# resize_queue 的 overflow_policy 缺省哨兵：省略时沿用当前策略；
# 显式传入（含 None）必须能通过 _check_overflow_policy 校验
_KEEP_POLICY = object()


class Result(dict):
    """结果对象：同时支持属性访问 (r.accepted) 与键访问 (r['accepted'])。"""

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)


class Snapshot(Result):
    """快照结果：仍是 Result，但 values 字段与 dict.values 方法同名，
    必须以数据描述符优先返回条目，保证 snapshot.values 与 snapshot['values']
    都取到 values 映射；其余三个字段一并显式声明属性访问。"""

    @property
    def values(self):
        return self['values']

    @property
    def events(self):
        return self['events']

    @property
    def seen(self):
        return self['seen']

    @property
    def max_queue(self):
        return self['max_queue']


def _check_duration(value, name):
    """ttl/window 必须是有限且不小于零的数值，布尔值不视为有效时长。"""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError('%s must be a finite non-negative number' % name)
    if not math.isfinite(value) or value < 0:
        raise ValueError('%s must be a finite non-negative number' % name)


def _check_expiry(value, name):
    """绝对到期时刻必须是非 bool 的有限 int/float。

    与 _check_duration 不同：到期时刻是时间轴上的绝对点，允许负数和
    早于当前时刻的值（恢复后按既有边界立即视为过期），不解释为相对时长。
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError('%s must be a finite number' % name)
    if not math.isfinite(value):
        raise ValueError('%s must be a finite number' % name)


def _check_max_queue(value):
    """max_queue 必须是 None 或非负整数，布尔值不视为有效上限。"""
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError('max_queue must be None or a non-negative integer')


def _check_discard_history_limit(value):
    """discard_history_limit 必须是 None 或非 bool 的非负整数。"""
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError('discard_history_limit must be None or a non-negative integer')


def _check_overflow_policy(value):
    """overflow_policy 只能是 'reject_new' 或 'drop_oldest' 字符串。"""
    if not isinstance(value, str) or value not in _OVERFLOW_POLICIES:
        raise ValueError("overflow_policy must be 'reject_new' or 'drop_oldest'")


def _check_clock_policy(value):
    """clock_policy 只能是 'allow_regression' 或 'reject_regression' 字符串。"""
    if not isinstance(value, str) or value not in _CLOCK_POLICIES:
        raise ValueError(
            "clock_policy must be 'allow_regression' or 'reject_regression'")


def _check_limit(value):
    """pop/peek 系列的 limit 必须是 None 或非 bool 的非负整数。"""
    if value is None:
        return
    # bool 是 int 的子类，必须显式排除；浮点数（含 2.0）同样拒绝
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError('limit must be None or a non-negative integer')


def _check_receipt(value):
    """回执必须是排除 bool 的正整数（从 1 起递增、不复用）。"""
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError('receipt must be a positive integer')


def _parse_batch(batch):
    """在读取时钟或改变任何状态前完整解析并校验批次。

    每项为 (dedupe, event, window) 三元组，或追加 event_ttl 的
    (dedupe, event, window, event_ttl) 四元组。批次不可迭代、条目长度不符
    或 window/event_ttl 非法时统一抛出 ValueError；dedupe 不可哈希、无法
    作为去重索引时抛出 TypeError。返回物化后的 (dedupe, event, window,
    event_ttl) 列表（三元组项的 event_ttl 为 None），供后续在同一时钟
    时刻逐项判定。
    """
    try:
        iterator = iter(batch)
    except TypeError:
        raise ValueError('batch must be an iterable of (dedupe, event, window) triples')
    entries = []
    for item in iterator:
        try:
            members = tuple(item)
        except TypeError:
            raise ValueError('each batch entry must be a (dedupe, event, window) triple')
        if len(members) == 3:
            dedupe, event, window = members
            event_ttl = None
        elif len(members) == 4:
            dedupe, event, window, event_ttl = members
            _check_duration(event_ttl, 'event_ttl')
        else:
            raise ValueError('each batch entry must be a (dedupe, event, window) triple')
        _check_duration(window, 'window')
        hash(dedupe)  # 不可哈希时原样抛出 TypeError
        entries.append((dedupe, event, window, event_ttl))
    return entries


def _parse_operation(item, allow_event_cleanup=False, allow_reads=False):
    """解析并物化单条带标签操作（apply_batch 与 replay_batch 共用）。

    操作必须是带标签的元组：('put', key, value, ttl)、('renew', key, ttl)、
    ('delete', key)、
    ('push', dedupe, event, window)、
    ('push_expiring', dedupe, event, window, event_ttl)、
    ('push_with_receipt', dedupe, event, window)、
    ('push_expiring_with_receipt', dedupe, event, window, event_ttl)、
    ('release_dedupe', dedupe)、
    ('cancel', receipt)、('pop_with_receipt',)、('peek_with_receipt',)、
    ('inspect',)、('cleanup',)、('cleanup_all_expired',) 或
    ('discard_expired_events',)；
    运行期容量调整 ('resize_queue', max_queue) 或
    ('resize_queue', max_queue, overflow_policy)（二元形式沿用当前策略）；
    allow_event_cleanup 为真时额外接受
    ('cleanup_expired_events',)；
    allow_reads 为真时再接受其余读取与出队路径的记录：('get', key)、
    ('get_with_reason', key)、('pop',)、
    ('pop_batch',)、('pop_batch', limit)、
    ('peek',)、('peek', limit)、
    ('pop_live_batch',)、('pop_live_batch', limit)、
    ('peek_live_batch',)、('peek_live_batch', limit) 与
    ('queue_status',)。回执的 pop_with_receipt/peek_with_receipt 与
    push_with_receipt/cancel 同属回执操作，apply_batch 与 replay_batch 都
    接受（不受 allow_reads 限制）。limit 只能是 None 或非 bool 的非负整数，
    receipt 只能是排除 bool 的正整数。
    条目不是元组、标签未知、元组长度不符或 ttl/window/event_ttl/limit/
    receipt/max_queue/overflow_policy 非法时统一抛出 ValueError；key/dedupe
    不可哈希、无法作为缓存索引时抛出 TypeError。返回物化后的操作元组。
    """
    if not isinstance(item, tuple) or len(item) == 0:
        raise ValueError('each operation must be a tagged tuple')
    tag = item[0]
    if tag == 'put':
        if len(item) != 4:
            raise ValueError("'put' operation must be ('put', key, value, ttl)")
        _, key, value, ttl = item
        _check_duration(ttl, 'ttl')
        hash(key)  # 不可哈希时原样抛出 TypeError
        return ('put', key, value, ttl)
    if tag == 'renew':
        if len(item) != 3:
            raise ValueError("'renew' operation must be ('renew', key, ttl)")
        _, key, ttl = item
        _check_duration(ttl, 'ttl')
        hash(key)
        return ('renew', key, ttl)
    if tag == 'delete':
        if len(item) != 2:
            raise ValueError("'delete' operation must be ('delete', key)")
        _, key = item
        hash(key)
        return ('delete', key)
    if tag == 'push':
        if len(item) != 4:
            raise ValueError("'push' operation must be ('push', dedupe, event, window)")
        _, dedupe, event, window = item
        _check_duration(window, 'window')
        hash(dedupe)
        return ('push', dedupe, event, window)
    if tag == 'push_expiring':
        if len(item) != 5:
            raise ValueError(
                "'push_expiring' operation must be ('push_expiring', dedupe, event, window, event_ttl)")
        _, dedupe, event, window, event_ttl = item
        _check_duration(window, 'window')
        _check_duration(event_ttl, 'event_ttl')
        hash(dedupe)
        return ('push_expiring', dedupe, event, window, event_ttl)
    if tag == 'push_with_receipt':
        if len(item) != 4:
            raise ValueError(
                "'push_with_receipt' operation must be ('push_with_receipt', dedupe, event, window)")
        _, dedupe, event, window = item
        _check_duration(window, 'window')
        hash(dedupe)
        return ('push_with_receipt', dedupe, event, window)
    if tag == 'push_expiring_with_receipt':
        if len(item) != 5:
            raise ValueError(
                "'push_expiring_with_receipt' operation must be "
                "('push_expiring_with_receipt', dedupe, event, window, event_ttl)")
        _, dedupe, event, window, event_ttl = item
        _check_duration(window, 'window')
        _check_duration(event_ttl, 'event_ttl')
        hash(dedupe)
        return ('push_expiring_with_receipt', dedupe, event, window, event_ttl)
    if tag == 'release_dedupe':
        # 释放去重占用与 cancel 同属两个批次都接受的操作（不受 allow_reads
        # 限制）：不读取时钟、不按时间判定，只按 seen 中是否存在记录生效
        if len(item) != 2:
            raise ValueError(
                "'release_dedupe' operation must be ('release_dedupe', dedupe)")
        _, dedupe = item
        hash(dedupe)
        return ('release_dedupe', dedupe)
    if tag == 'cancel':
        if len(item) != 2:
            raise ValueError("'cancel' operation must be ('cancel', receipt)")
        _, receipt = item
        # receipt 非法（含 bool）统一 ValueError，在读时钟或改状态之前抛出
        _check_receipt(receipt)
        return ('cancel', receipt)
    if tag in ('pop_with_receipt', 'peek_with_receipt'):
        # 回执的 pop/peek 属于 receipt 相关操作：apply_batch 与 replay_batch
        # 都接受（不受 allow_reads 限制），且都不带参数
        if len(item) != 1:
            raise ValueError("'%s' operation must be ('%s',)" % (tag, tag))
        return (tag,)
    if tag == 'inspect':
        # 只读诊断与回执 pop/peek 同属两个批次都接受的操作（不受
        # allow_reads 限制），且不带参数；apply_batch 复用批次时钟，
        # replay_batch 以记录 timestamp 作为观察时刻
        if len(item) != 1:
            raise ValueError("'inspect' operation must be ('inspect',)")
        return ('inspect',)
    if tag == 'cleanup':
        if len(item) != 1:
            raise ValueError("'cleanup' operation must be ('cleanup',)")
        return ('cleanup',)
    if tag == 'cleanup_all_expired':
        if len(item) != 1:
            raise ValueError(
                "'cleanup_all_expired' operation must be ('cleanup_all_expired',)")
        return ('cleanup_all_expired',)
    if tag == 'discard_expired_events':
        if len(item) != 1:
            raise ValueError(
                "'discard_expired_events' operation must be ('discard_expired_events',)")
        return ('discard_expired_events',)
    if tag == 'resize_queue':
        # 二元形式省略 overflow_policy（物化为 None 表示沿用当前策略）；
        # 三元形式的策略必须显式合法，max_queue 与构造入口同一校验
        if len(item) == 2:
            max_queue = item[1]
            policy = None
        elif len(item) == 3:
            max_queue, policy = item[1], item[2]
            _check_overflow_policy(policy)
        else:
            raise ValueError(
                "'resize_queue' operation must be ('resize_queue', max_queue) "
                "or ('resize_queue', max_queue, overflow_policy)")
        _check_max_queue(max_queue)
        return ('resize_queue', max_queue, policy)
    if tag == 'cleanup_expired_events' and allow_event_cleanup:
        if len(item) != 1:
            raise ValueError(
                "'cleanup_expired_events' operation must be ('cleanup_expired_events',)")
        return ('cleanup_expired_events',)
    if allow_reads:
        if tag == 'get':
            if len(item) != 2:
                raise ValueError("'get' operation must be ('get', key)")
            _, key = item
            hash(key)
            return ('get', key)
        if tag == 'get_with_reason':
            if len(item) != 2:
                raise ValueError("'get_with_reason' operation must be ('get_with_reason', key)")
            _, key = item
            hash(key)
            return ('get_with_reason', key)
        if tag == 'pop':
            if len(item) != 1:
                raise ValueError("'pop' operation must be ('pop',)")
            return ('pop',)
        if tag in ('pop_batch', 'peek', 'pop_live_batch', 'peek_live_batch'):
            if len(item) == 1:
                limit = None
            elif len(item) == 2:
                limit = item[1]
                _check_limit(limit)
            else:
                raise ValueError(
                    "'%s' operation must be ('%s',) or ('%s', limit)" % (tag, tag, tag))
            return (tag, limit)
        if tag == 'queue_status':
            if len(item) != 1:
                raise ValueError("'queue_status' operation must be ('queue_status',)")
            return ('queue_status',)
    raise ValueError('unknown operation tag: %r' % (tag,))


def _parse_apply_batch(batch):
    """在读取时钟或改变任何状态前完整解析并校验事务批次。

    批次必须可迭代，每项为带标签的元组：
    ('put', key, value, ttl)、('renew', key, ttl)、('delete', key)、
    ('push', dedupe, event, window)、
    ('push_expiring', dedupe, event, window, event_ttl)、
    ('push_with_receipt', dedupe, event, window)、
    ('push_expiring_with_receipt', dedupe, event, window, event_ttl)、
    ('release_dedupe', dedupe)、
    ('cancel', receipt)、('inspect',)、('cleanup',)、
    ('cleanup_all_expired',)、('discard_expired_events',)、
    ('resize_queue', max_queue[, overflow_policy]) 或
    ('cleanup_expired_events',)。
    批次不可迭代、条目不是元组、标签未知、元组长度不符或 ttl/window/
    event_ttl/receipt/max_queue/overflow_policy 非法时统一抛出 ValueError；
    key/dedupe 不可哈希、无法作为缓存索引时抛出 TypeError。物化后的操作列表供
    调用方在同一时钟时刻顺序执行。
    """
    try:
        iterator = iter(batch)
    except TypeError:
        raise ValueError('operations must be an iterable of operation tuples')
    return [_parse_operation(item, allow_event_cleanup=True) for item in iterator]


def _parse_replay_batch(records):
    """在读取时钟或改变任何状态前完整解析并校验回放记录。

    records 必须可迭代，每项为 (timestamp, operation) 二元结构：timestamp
    只能是非 bool 的有限 int/float，且按非递减顺序出现（同一时间戳共享
    边界，时间倒退抛出 ValueError）；operation 为带标签元组，除
    apply_batch 的 put/renew/delete/push/push_expiring/push_with_receipt/
    push_expiring_with_receipt/release_dedupe/cancel/inspect/cleanup/
    cleanup_all_expired/discard_expired_events/resize_queue 与
    ('cleanup_expired_events',) 外，还接受读取与出队路径的记录：('get', key)、
    ('get_with_reason', key)、('pop',)、('pop_with_receipt',)、
    ('pop_batch'[, limit])、('peek'[, limit])、('peek_with_receipt',)、
    ('pop_live_batch'[, limit])、('peek_live_batch'[, limit]) 与
    ('queue_status',)，其中 limit 只能是 None 或非 bool 的非负整数，
    receipt 只能是排除 bool 的正整数。
    records 不可迭代、记录不是二元结构、时间戳非法或倒退、操作结构/标签/
    参数数量/时长/limit/receipt/max_queue/overflow_policy 非法时统一抛出
    ValueError；key/dedupe 不可哈希时原样抛出 TypeError。返回物化后的
    (timestamp, operation) 列表，供调用方按各自记录时刻顺序回放。
    """
    try:
        iterator = iter(records)
    except TypeError:
        raise ValueError('records must be an iterable of (timestamp, operation) pairs')
    parsed = []
    previous = None
    for item in iterator:
        try:
            members = tuple(item)
        except TypeError:
            raise ValueError('each record must be a (timestamp, operation) pair')
        if len(members) != 2:
            raise ValueError('each record must be a (timestamp, operation) pair')
        timestamp, operation = members
        # 时间戳与绝对到期时刻同类：非 bool 的有限 int/float，允许负数
        _check_expiry(timestamp, 'timestamp')
        if previous is not None and timestamp < previous:
            raise ValueError('timestamps must be in non-decreasing order')
        previous = timestamp
        parsed.append((timestamp, _parse_operation(
            operation, allow_event_cleanup=True, allow_reads=True)))
    return parsed


def _parse_snapshot(snapshot):
    """在读取时钟或改变任何状态前完整解析并校验快照。

    快照必须是含 values、events、seen、max_queue 四个字段的映射，可在此
    基础上增加与 events 对齐的 event_expiries 字段、overflow_policy 字段、
    丢弃审计历史的 discard_history / discard_history_limit 字段、回执的
    event_receipts / next_receipt 字段以及实时时钟保护的
    clock_policy / last_clock_time 字段：
    values 为 key -> (value, expires_at) 的映射，seen 为去重键 ->
    绝对到期时间的映射，events 为事件列表（按 FIFO 顺序），max_queue 为
    None 或非负整数，event_expiries 为与 events 等长的列表，每项为
    None（无事件 TTL）或有限的绝对到期时刻，overflow_policy 为
    'reject_new' 或 'drop_oldest'（缺省时按 'reject_new' 解释，旧四字段
    与五字段快照因此保持兼容），event_receipts 为与 events 等长的对齐列表，
    每项为 None（旧事件，无回执）或排除 bool 的正整数回执，next_receipt 为
    排除 bool 的正整数，且必须严格大于 event_receipts 中的任一回执（保证
    恢复后继续递增、不复用；旧快照缺这两个字段时回执按全 None、计数器按
    1 解释，旧事件因此不可被 cancel 命中），discard_history 为按丢弃先后
    排列的列表，每项必须是恰好含 event、reason、timestamp 三个字段的映射：
    reason 只能是 'event_ttl' 或 'queue_full'，timestamp 只能是非 bool 的
    有限 int/float（允许负数），event 可为任意对象（含 None）；
    discard_history_limit 为 None 或非负整数。两个历史字段均可省略：
    缺 discard_history 按空历史解释，缺 discard_history_limit 按无限容量
    解释，只给出其一时另一项按缺省解释。event_metadata 为与 events 等长的
    对齐列表，每项必须是恰好含 dedupe_known、dedupe_key、dedupe_expires_at
    三个字段的映射：dedupe_known 只能是 bool（True 表示去重键已知，False
    表示未知键旧条目，借此区分真实的 None 键与未知键）；dedupe_key 为入队
    时的去重键，必须可哈希（含 None，None 是真实去重键而非未知标记）；
    dedupe_expires_at 为 None 或非 bool 的有限 int/float 绝对到期点（允许
    负数与已过期时刻）。dedupe_known 为 False 的条目一律按未知键旧条目
    恢复（dedupe_key/dedupe_expires_at 规范为 None）。缺 event_metadata
    字段的旧快照按全部条目 dedupe_known=False 解释，保持兼容。三类到期时间（values 的
    expires_at、seen 的到期时刻、event_expiries 的非 None 项）都只能是非
    bool 的有限 int/float，允许负数与已过期时刻，不解释为相对时长。字段
    缺失或多余、非映射/列表容器、二元组结构不符、event_expiries/
    event_receipts/event_metadata 长度不一致、event_metadata 条目结构/
    known 标记/到期时间非法或键不可哈希、event_receipts/next_receipt 回执非法或
    next_receipt 不大于在队回执、overflow_policy 非法、历史条目结构/原因/
    时间戳非法、discard_history_limit 非法或任一到期值为
    NaN/无穷/字符串/复合对象、max_queue 非法或 max_queue 为非负整数而事件
    条目数超过该上限（零上限只接受空队列，判断针对实际条目数而非过期与否，
    超容快照整体拒绝而不截断或挤出）时统一抛出 ValueError；clock_policy
    缺省时按 'allow_regression' 解释（旧快照因此保持兼容），显式给出时只能
    是 'allow_regression' 或 'reject_regression'；last_clock_time 与
    clock_policy 成对出现，只能是 None 或非 bool 的有限 int/float（允许负数，
    与绝对到期时刻同类），只给出其一时另一项不做缺省推断而整体拒绝；
    allow_regression 快照的水位恒按 None 恢复（即使给出 last_clock_time 也
    不接受带水位的 allow_regression 快照，避免旧的过期记录在策略语义下被
    重新判为有效）；键不可哈希时
    原样抛出 TypeError。校验期间一次性物化为全新的 dict/list/deque，供调用
    方随后整体替换状态。
    """
    if not isinstance(snapshot, Mapping):
        raise ValueError('snapshot must be a mapping with values, events, seen, max_queue')
    fields = frozenset(snapshot.keys())
    if not (_SNAPSHOT_FIELDS <= fields <= _SNAPSHOT_FIELDS_ALL):
        raise ValueError('snapshot must contain exactly values, events, seen, max_queue')

    raw_values = snapshot['values']
    raw_seen = snapshot['seen']
    raw_events = snapshot['events']
    if not isinstance(raw_values, Mapping):
        raise ValueError('snapshot values must be a mapping')
    if not isinstance(raw_seen, Mapping):
        raise ValueError('snapshot seen must be a mapping')
    if not isinstance(raw_events, list):
        raise ValueError('snapshot events must be a list')
    max_queue = snapshot['max_queue']
    _check_max_queue(max_queue)
    # 有限容量快照的边界：事件条目数不得超过 max_queue（max_queue 为 0 时
    # 只能恢复空队列）。容量判断针对实际事件条目，与条目是否已过期无关；
    # 超容快照整体拒绝，不截断、不挤出、不接受后等待后续写入处理。
    if max_queue is not None and len(raw_events) > max_queue:
        raise ValueError('snapshot events must not exceed max_queue')

    values = {}
    for key, item in raw_values.items():
        if not isinstance(item, tuple) or len(item) != 2:
            raise ValueError('each values entry must be a (value, expires_at) pair')
        hash(key)  # 不可哈希时原样抛出 TypeError
        _check_expiry(item[1], 'expires_at')
        values[key] = item
    seen = {}
    for key, expiry in raw_seen.items():
        hash(key)  # 不可哈希时原样抛出 TypeError
        _check_expiry(expiry, 'seen expiry')
        seen[key] = expiry
    # list() 物化事件副本；值与事件对象按既有语义保留引用
    events = deque(raw_events)
    if 'event_expiries' in snapshot:
        raw_expiries = snapshot['event_expiries']
        if not isinstance(raw_expiries, list):
            raise ValueError('snapshot event_expiries must be a list')
        if len(raw_expiries) != len(raw_events):
            raise ValueError('snapshot event_expiries must align with events in length')
        event_expiries = deque()
        for expiry in raw_expiries:
            if expiry is not None:
                _check_expiry(expiry, 'event_expiries entry')
            event_expiries.append(expiry)
    else:
        # 旧格式快照：所有事件均无 TTL
        event_expiries = deque([None] * len(raw_events))
    if 'event_receipts' in snapshot:
        raw_receipts = snapshot['event_receipts']
        if not isinstance(raw_receipts, list):
            raise ValueError('snapshot event_receipts must be a list')
        if len(raw_receipts) != len(raw_events):
            raise ValueError('snapshot event_receipts must align with events in length')
        event_receipts = deque()
        max_receipt = 0
        for receipt in raw_receipts:
            if receipt is not None:
                # 在队回执必须是排除 bool 的正整数
                _check_receipt(receipt)
                if receipt > max_receipt:
                    max_receipt = receipt
            event_receipts.append(receipt)
        # next_receipt 与 event_receipts 是一对：给了其一就必须给出另一，
        # 且计数器必须严格大于任一在队回执，保证恢复后继续递增、不复用
        if 'next_receipt' not in snapshot:
            raise ValueError('snapshot next_receipt required with event_receipts')
        next_receipt = snapshot['next_receipt']
        _check_receipt(next_receipt)
        if next_receipt <= max_receipt:
            raise ValueError('snapshot next_receipt must be greater than every event receipt')
    elif 'next_receipt' in snapshot:
        # 只给计数器而不给对齐列表不构成一致的回执状态
        raise ValueError('snapshot event_receipts required with next_receipt')
    else:
        # 旧格式快照：旧事件无回执（不可被 cancel 命中），新回执从 1 分配
        event_receipts = deque([None] * len(raw_events))
        next_receipt = 1
    if 'event_metadata' in snapshot:
        raw_metadata = snapshot['event_metadata']
        if not isinstance(raw_metadata, list):
            raise ValueError('snapshot event_metadata must be a list')
        if len(raw_metadata) != len(raw_events):
            raise ValueError('snapshot event_metadata must align with events in length')
        event_metadata = deque()
        for entry in raw_metadata:
            if not isinstance(entry, Mapping) or \
                    frozenset(entry.keys()) != _METADATA_ENTRY_FIELDS:
                raise ValueError(
                    'each event_metadata entry must be a mapping with '
                    'dedupe_known, dedupe_key, dedupe_expires_at')
            known = entry['dedupe_known']
            # known 标记只接受真正的 bool，不用真值表推断
            if not isinstance(known, bool):
                raise ValueError('event_metadata dedupe_known must be a bool')
            key = entry['dedupe_key']
            expiry = entry['dedupe_expires_at']
            if key is not None:
                # 去重键必须可哈希；元数据校验统一报 ValueError（含不可哈希键）
                try:
                    hash(key)
                except TypeError:
                    raise ValueError(
                        'event_metadata dedupe_key must be hashable')
            if expiry is not None:
                _check_expiry(expiry, 'event_metadata dedupe_expires_at')
            if known:
                # 已知键条目：None 键是真实的 None 去重键，原样保留
                event_metadata.append(Result(
                    dedupe_known=True, dedupe_key=key,
                    dedupe_expires_at=expiry))
            else:
                # 未知键旧条目：相关字段一律规范为 None
                event_metadata.append(Result(
                    dedupe_known=False, dedupe_key=None,
                    dedupe_expires_at=None))
    else:
        # 旧格式快照：所有事件的去重键未知，相关字段为 None
        event_metadata = deque(
            Result(dedupe_known=False, dedupe_key=None, dedupe_expires_at=None)
            for _ in range(len(raw_events)))
    # 缺省按 'reject_new' 解释；显式给出时校验合法性
    overflow_policy = snapshot.get('overflow_policy', 'reject_new')
    _check_overflow_policy(overflow_policy)
    # 丢弃审计历史：缺字段按空历史解释；给出时逐条校验结构并物化为独立 Result
    discard_history = deque()
    if 'discard_history' in snapshot:
        raw_history = snapshot['discard_history']
        if not isinstance(raw_history, list):
            raise ValueError('snapshot discard_history must be a list')
        for entry in raw_history:
            if not isinstance(entry, Mapping) or frozenset(entry.keys()) != _DISCARD_ENTRY_FIELDS:
                raise ValueError(
                    'each discard_history entry must be a mapping with event, reason, timestamp')
            reason = entry['reason']
            if not isinstance(reason, str) or reason not in _DISCARD_REASONS:
                raise ValueError("discard_history reason must be 'event_ttl' or 'queue_full'")
            timestamp = entry['timestamp']
            _check_expiry(timestamp, 'discard_history timestamp')
            # 事件对象按既有接口语义保留引用，条日本身物化为全新 Result
            discard_history.append(
                Result(event=entry['event'], reason=reason, timestamp=timestamp))
    # 缺字段按无限容量解释；显式给出时校验 None / 非负整数（拒绝布尔）
    discard_history_limit = snapshot.get('discard_history_limit', None)
    _check_discard_history_limit(discard_history_limit)
    if discard_history_limit is not None and len(discard_history) > discard_history_limit:
        # 与构造容量保持同一不变量：只保留最近的记录（缓存自身产生的快照
        # 总已满足该不变量，此处仅规范手工构造的快照）。注意 -0 == 0，
        # 不能用 [-0:]（等价于整个列表），零容量需显式清空。
        if discard_history_limit == 0:
            discard_history = deque()
        else:
            discard_history = deque(list(discard_history)[-discard_history_limit:])
    # 实时时钟保护策略：缺省按 allow_regression 解释，旧快照保持兼容；
    # 显式给出时校验合法性。clock_policy 与 last_clock_time 成对出现，
    # 缺前者时后者也不被接受（默认形状的快照不含水位字段），缺后者时
    # reject_regression 无法表达明确水位，同样整体拒绝。
    clock_policy = snapshot.get('clock_policy', 'allow_regression')
    _check_clock_policy(clock_policy)
    if 'clock_policy' in snapshot:
        if 'last_clock_time' not in snapshot:
            raise ValueError('snapshot last_clock_time required with clock_policy')
        last_clock_time = snapshot['last_clock_time']
        if last_clock_time is not None:
            # 非 None 水位与绝对到期时刻同类：非 bool 的有限 int/float，允许负数
            _check_expiry(last_clock_time, 'last_clock_time')
        if clock_policy == 'allow_regression' and last_clock_time is not None:
            # 允许回退模式不维护水位：带水位的该类快照语义不一致，整体拒绝
            raise ValueError(
                'snapshot last_clock_time must be None for allow_regression')
    elif 'last_clock_time' in snapshot:
        raise ValueError('snapshot clock_policy required with last_clock_time')
    else:
        # 旧格式快照：沿用允许回退行为且无时间水位
        last_clock_time = None
    return (values, events, event_expiries, seen, max_queue,
            overflow_policy, discard_history, discard_history_limit,
            event_receipts, next_receipt, clock_policy, last_clock_time,
            event_metadata)


class EventCache:
    def __init__(self, clock, max_queue=None, overflow_policy='reject_new',
                 discard_history_limit=None, clock_policy='allow_regression'):
        # 策略与容量在校验通过前不触碰任何状态，也不读取时钟
        _check_max_queue(max_queue)
        _check_overflow_policy(overflow_policy)
        _check_discard_history_limit(discard_history_limit)
        _check_clock_policy(clock_policy)
        self.clock = clock
        self.max_queue = max_queue
        self.overflow_policy = overflow_policy
        self.clock_policy = clock_policy
        # reject_regression 下最近一次成功采样的实时时钟水位；首次采样前为
        # None。allow_regression 始终为 None：不比较、不推进。
        self._last_clock_time = None
        self.values = {}
        self.events = deque()
        # 与 events 逐元素对齐：None 表示无事件 TTL，否则为绝对到期时刻
        self.event_expiries = deque()
        # 与 events 逐元素对齐：None 表示旧事件（无回执，cancel 不命中），
        # 否则为该事件分配的、从 1 起递增且不复用的正整数回执
        self.event_receipts = deque()
        # 与 events 逐元素对齐：每项为含 dedupe_known/dedupe_key/
        # dedupe_expires_at 的 Result；经 push 入口接受的事件记为已知键
        # （dedupe_known=True，含真实的 None 键），旧快照恢复的事件为未知键
        self.event_metadata = deque()
        # 下一个待分配的回执；仅在入队被接受时自增，拒绝、出队与取消都不复用
        self._next_receipt = 1
        self.seen = {}
        # 丢弃审计历史：按丢弃先后排列，每项为含
        # event/reason/timestamp 的独立 Result；None 容量表示无限
        self._discard_history = deque()
        self.discard_history_limit = discard_history_limit

    def _record_discard(self, event, reason, now):
        # 追加一条丢弃审计记录。now 是触发该丢弃动作的那次观察时刻，由
        # 调用方显式传入（实时路径为当时钟读数，回放路径为记录时间戳），
        # 本方法自身绝不读取注入时钟。历史容量有限时先淘汰最早记录；容量
        # 为零时 deque 追加后立即弹出，效果为不留存。事件为 None 同样记录。
        history = self._discard_history
        history.append(Result(event=event, reason=reason, timestamp=now))
        limit = self.discard_history_limit
        if limit is not None and len(history) > limit:
            history.popleft()

    def _read_clock(self):
        """实时路径唯一的注入时钟采样点，并在 reject_regression 下守护水位。

        allow_regression 时直接返回时钟读数，不比较也不维护水位，沿用既有
        的允许回退行为。reject_regression 时：首次采样（水位为 None）只记录
        读数；此后读数等于水位可继续，小于水位抛出 ClockRegressionError，
        且在抛出前不触碰任何状态（水位与数据均保持原样）；时钟自身抛出的
        异常原样传播，同样不改变水位与数据。采样成功即推进水位，即使调用
        方随后得到的是 missing、expired、queue_full 或没有可清理项。
        """
        now = self.clock()
        if self.clock_policy == 'reject_regression':
            last = self._last_clock_time
            if last is not None and now < last:
                raise ClockRegressionError(now, last)
            self._last_clock_time = now
        return now

    def clock_status(self):
        """不读取时钟地报告实时时钟保护状态。

        纯查询：永不读取注入时钟、永不推进水位、不改变任何状态。返回
        Result(clock_policy=当前策略, last_time=最近一次成功采样的水位)，
        首次采样前 last_time 为 None；allow_regression 下 last_time 恒为
        None。
        """
        return Result(clock_policy=self.clock_policy,
                      last_time=self._last_clock_time)

    def _inspect_at(self, now):
        # 在指定观察时刻生成只读诊断快照：本方法绝不读取注入时钟，也绝不
        # 删除、移动、续期或补写任何 values、seen、events、event_expiries、
        # 回执或丢弃历史（逐项扫描而不 popleft/写回）。到期边界与全部既有
        # 路径一致：到期点 <= now 即视为到期，但到期项仍占据容器，因此计入
        # 对应的 expired_* 计数而不在此清理。未设置事件 TTL（expiry 为
        # None）的事件始终计入 live_event_count，也不参与 next_event_expiry。
        value_count = 0
        expired_value_count = 0
        next_value_expiry = None
        for _key, (_value, expiry) in self.values.items():
            if expiry <= now:
                expired_value_count += 1
            else:
                value_count += 1
                if next_value_expiry is None or expiry < next_value_expiry:
                    next_value_expiry = expiry
        dedupe_count = 0
        expired_dedupe_count = 0
        next_dedupe_expiry = None
        for expiry in self.seen.values():
            if expiry <= now:
                expired_dedupe_count += 1
            else:
                dedupe_count += 1
                if next_dedupe_expiry is None or expiry < next_dedupe_expiry:
                    next_dedupe_expiry = expiry
        live_event_count = 0
        expired_event_count = 0
        next_event_expiry = None
        for expiry in self.event_expiries:
            if expiry is None:
                # 未设置事件 TTL 的事件始终存活，且没有到期点
                live_event_count += 1
            elif expiry <= now:
                expired_event_count += 1
            else:
                live_event_count += 1
                if next_event_expiry is None or expiry < next_event_expiry:
                    next_event_expiry = expiry
        # queue_size 是排队条目总数（含仍占槽位的到期事件）；
        # discard_history_size 只报告长度，不返回历史条目本身
        return Result(
            observed_at=now,
            value_count=value_count,
            expired_value_count=expired_value_count,
            dedupe_count=dedupe_count,
            expired_dedupe_count=expired_dedupe_count,
            queue_size=len(self.events),
            live_event_count=live_event_count,
            expired_event_count=expired_event_count,
            next_value_expiry=next_value_expiry,
            next_dedupe_expiry=next_dedupe_expiry,
            next_event_expiry=next_event_expiry,
            discard_history_size=len(self._discard_history),
        )

    def inspect(self):
        """只读诊断：单次读取注入时钟，报告容器内存活与到期占用情况。

        整次调用只从注入时钟读取一次作为 observed_at，随后不删除、不移动、
        不续期、不补写任何 values、seen、events/event_expiries、回执或丢弃
        历史；调用后再执行读取、清理或 FIFO 出队，结果与未调用 inspect 时
        完全一致。到期边界与既有路径一致：到期点 <= observed_at 即到期。
        value_count/dedupe_count 只计未到期项；expired_value_count/
        expired_dedupe_count/expired_event_count 只计仍占据容器的到期项
        （惰性过期尚未清理的记录照常计数）。queue_size 为排队条目总数
        （含仍占槽位的到期事件）；未设置事件 TTL 的事件始终计入
        live_event_count，已到期但仍在队的带 TTL 事件计入
        expired_event_count，二者之和等于 queue_size。三个 next_* 字段只
        给出严格晚于 observed_at 的最早到期点，没有则为 None；
        discard_history_size 只报告丢弃历史条数。空容器仍返回全部字段。
        reject_regression 下时钟回退时抛出 ClockRegressionError 且状态与
        水位均不变；时钟自身抛出的其他异常原样传播。
        """
        now = self._read_clock()
        return self._inspect_at(now)

    def _put_at(self, key, value, ttl, now):
        # 以写入时刻加 ttl 记录到期点，并替换同 key 旧值；ttl 由调用方先行校验
        self.values[key] = (value, now + ttl)

    def put(self, key, value, ttl):
        _check_duration(ttl, 'ttl')
        now = self._read_clock()
        self._put_at(key, value, ttl, now)

    def _get_at(self, key, now):
        item = self.values.get(key)
        if item is None:
            return None
        value, expiry = item
        # 到期点小于或等于判定时刻即视为过期
        if expiry <= now:
            self.values.pop(key, None)
            return None
        return value

    def get(self, key):
        # 保持既有时间约定：键不存在（或值记录缺失）时不读取注入时钟
        item = self.values.get(key)
        if item is None:
            return None
        return self._get_at(key, self._read_clock())

    def _get_with_reason_at(self, key, now):
        # 与 _get_at 共用同一 <= 过期边界与到期即删清理，但结果区分三种情形；
        # value 为 None、False 等假值时只要记录仍存活就以 found=True 原样返回。
        value, expiry = self.values[key]
        if expiry <= now:
            # 与 get 一样只移除该键，不触发 values/seen 的批量清理
            self.values.pop(key, None)
            return Result(found=False, value=None, reason='expired')
        return Result(found=True, value=value, reason=None)

    def get_with_reason(self, key):
        """带原因的诊断读取：区分缺失、本次读取时已过期与仍然有效三种结果。

        返回 Result(found=, value=, reason=)：键不存在时为
        Result(found=False, value=None, reason='missing')，且不读取注入时钟；
        键存在时只读取一次注入时钟，到期点 expires_at <= 当前时刻即视为过期，
        过期记录与 get 一样只从 values 删除该键并返回
        Result(found=False, value=None, reason='expired')，随后再次读取同键
        按 missing 返回；未过期记录返回
        Result(found=True, value=原值, reason=None)，原值为 None、False、0 等
        假值时 found 同样为 True 且原值原样保留。

        纯诊断入口：不触碰 seen、events、event_expiries 与队列容量，不触发
        任何批量清理。key 不可哈希时成员判定原样抛出 TypeError，此时尚未读取
        时钟也未改变状态；时钟抛出的异常原样传播且状态保持不变。
        """
        # 与 get 相同的时钟约定：键不存在（或值记录缺失）时不读取注入时钟
        if key not in self.values:
            return Result(found=False, value=None, reason='missing')
        return self._get_with_reason_at(key, self._read_clock())

    def _renew_at(self, key, ttl, now):
        # 在指定观察时刻续期：不替换原值、不触碰事件队列。ttl 由调用方先行
        # 校验。键不存在时直接报告 missing（实时路径因此不读取时钟）；到期点
        # <= 观察时刻与 get 同一边界，过期则删除该值并报告 expired；仍有效时
        # 原值（含 None、False、0 等假值）原样保留，仅把到期点改为 now + ttl。
        # 只影响 values 这一条记录，不清 seen、不动事件与容量。
        item = self.values.get(key)
        if item is None:
            return Result(renewed=False, value=None, reason='missing',
                          expires_at=None)
        value, expiry = item
        if expiry <= now:
            self.values.pop(key, None)
            return Result(renewed=False, value=None, reason='expired',
                          expires_at=None)
        expires_at = now + ttl
        self.values[key] = (value, expires_at)
        return Result(renewed=True, value=value, reason=None,
                      expires_at=expires_at)

    def renew(self, key, ttl):
        """在不替换值、不触碰事件队列的情况下延长仍存活键的有效期。

        ttl 与 put 同一校验口径：只接受有限且不小于零的数值，布尔值、负数、
        NaN、无穷值与其他类型统一抛出 ValueError；key 不可哈希时成员判定原样
        抛出 TypeError。校验失败时不读取时钟、不改变任何状态。

        键不存在时不读取注入时钟，返回
        Result(renewed=False, value=None, reason='missing', expires_at=None)；
        键存在时只读取一次注入时钟，沿用到期点 <= 观察时刻即过期的边界，
        已过期就与 get 一样只从 values 删除该键，返回
        Result(renewed=False, value=None, reason='expired', expires_at=None)，
        随后再次续期同键按 missing 返回；仍有效时保留原值（None、False、0 等
        假值同样原样保留），把绝对到期点设为观察时刻 + ttl，返回
        Result(renewed=True, value=原值, reason=None, expires_at=新到期点)，
        新到期点是时间轴上的绝对点，可直接用于确定性回放。

        续期只影响 values：不清理也不延长 seen 去重窗口，不触碰 events、
        event_expiries、容量与溢出策略、回执与 discard_history。时钟自身抛出
        的异常或 reject_regression 拒绝时钟回退时原样传播，抛出前状态完全
        保留（含不更新到期点）。
        """
        _check_duration(ttl, 'ttl')
        # 与 get/get_with_reason 相同的时钟约定：键不存在时不读取注入时钟
        if key not in self.values:
            return Result(renewed=False, value=None, reason='missing',
                          expires_at=None)
        return self._renew_at(key, ttl, self._read_clock())

    def delete(self, key):
        # 结果只表达键是否存在，与取出的值无关：value 为 None、False、0、''
        # 等假值，或记录虽已到期但仍留在 values 中，都一样移除并返回 True；
        # 仅当键本就不存在时返回 False。
        # 纯移除操作：不读取注入时钟、不触发 values/seen 的批量清理，FIFO 队列、
        # 去重占用与容量状态一律不变。key 不可哈希时成员判定原样抛出 TypeError，
        # 此时尚未发生任何删除，缓存状态保持不变。
        if key not in self.values:
            return False
        del self.values[key]
        return True

    def _cleanup_at(self, now):
        values_removed = 0
        for key in [k for k, (_, expiry) in self.values.items() if expiry <= now]:
            del self.values[key]
            values_removed += 1
        dedupe_removed = 0
        for key in [k for k, expiry in self.seen.items() if expiry <= now]:
            del self.seen[key]
            dedupe_removed += 1
        # 已排入队列的事件不受影响
        return values_removed, dedupe_removed

    def cleanup(self):
        now = self._read_clock()
        values_removed, dedupe_removed = self._cleanup_at(now)
        return Result(values_removed=values_removed, dedupe_removed=dedupe_removed)

    def _cleanup_events_at(self, now):
        # 在指定时刻移除所有已到期的带 TTL 事件；到期边界与 values/seen 一致：
        # 到期点 <= 当前时刻即移除。未到期事件与未设置事件 TTL 的旧事件一律
        # 保留且相对顺序不变；values、seen 与 max_queue 不受影响。每个被移除
        # 事件按触发清理的观察时刻 now 追加一条 reason='event_ttl' 的审计历史。
        # 被移除事件的回执与去重元数据一并出列：回执不复用，此后对其
        # cancel 报 missing。
        kept_events = deque()
        kept_expiries = deque()
        kept_receipts = deque()
        kept_metadata = deque()
        events_removed = 0
        for event, expiry, receipt, metadata in zip(
                self.events, self.event_expiries, self.event_receipts,
                self.event_metadata):
            if expiry is not None and expiry <= now:
                events_removed += 1
                self._record_discard(event, 'event_ttl', now)
            else:
                kept_events.append(event)
                kept_expiries.append(expiry)
                kept_receipts.append(receipt)
                kept_metadata.append(metadata)
        self.events = kept_events
        self.event_expiries = kept_expiries
        self.event_receipts = kept_receipts
        self.event_metadata = kept_metadata
        return events_removed

    def cleanup_expired_events(self):
        """单次读取时钟，移除所有已到期的带 TTL 事件。

        到期边界与 values/seen 一致：到期点 <= 当前时刻即移除。未到期事件
        与未设置事件 TTL 的旧事件一律保留且相对顺序不变；values、seen 与
        max_queue 不受影响。返回 Result(events_removed=移除数量)。每个被
        移除事件以本次时钟读数为 timestamp 追加一条 reason='event_ttl' 的
        丢弃审计历史（受 discard_history_limit 容量约束）。
        """
        now = self._read_clock()
        return Result(events_removed=self._cleanup_events_at(now))

    def _discard_expired_events_at(self, now):
        # 在指定时刻扫描整个队列，移除所有带 TTL 且已到期的事件，并返回按
        # 原 FIFO 顺序排列的丢弃记录。到期边界与 values/seen/
        # cleanup_expired_events 一致：到期点 <= 判定时刻即过期。未设置
        # event_ttl 或尚未到期的事件一律保留且相对顺序不变，被移除事件立即
        # 释放容量；values、seen 与 max_queue 不受影响。
        kept_events = deque()
        kept_expiries = deque()
        kept_receipts = deque()
        kept_metadata = deque()
        discarded = []
        for event, expiry, receipt, metadata in zip(
                self.events, self.event_expiries, self.event_receipts,
                self.event_metadata):
            if expiry is not None and expiry <= now:
                # 事件值为 None 也保留该条丢弃记录；同时写入审计历史。
                # discarded 形状保持不变（只有 event/reason），不带回执；
                # 回执与去重元数据随事件一并出列且不复用。
                record = Result(event=event, reason='event_ttl')
                discarded.append(record)
                self._record_discard(event, 'event_ttl', now)
            else:
                kept_events.append(event)
                kept_expiries.append(expiry)
                kept_receipts.append(receipt)
                kept_metadata.append(metadata)
        self.events = kept_events
        self.event_expiries = kept_expiries
        self.event_receipts = kept_receipts
        self.event_metadata = kept_metadata
        return discarded

    def discard_expired_events(self):
        """审计并清理队列中所有已到期的带 TTL 事件，返回丢弃明细。

        单次读取注入时钟作为观察时刻（即使队列为空也沿用一次读取的约定），
        按当前 FIFO 顺序扫描整个队列：设置了 event_ttl 且绝对到期点 <= 观察
        时刻的事件被移除并立即释放容量，未设置事件 TTL 或尚未到期的事件一律
        保留且相对顺序不变。时钟抛出的异常原样传播，队列、到期对齐信息与
        其他缓存状态保持不变。

        纯队列清理：不读取或改变 values、seen 与 max_queue，也不延长或删除
        去重窗口。返回 Result(events_removed=移除数量, discarded=丢弃记录
        列表)，discarded 中每项为 Result(event=原事件值, reason='event_ttl')，
        顺序与被删除事件一致，事件值为 None 时同样保留该条记录；没有新的
        到期事件时重复调用返回零计数和空列表。每个被移除事件还会以本次观察
        时刻为 timestamp 追加一条 reason='event_ttl' 的丢弃审计历史（受
        构造时 discard_history_limit 容量约束），discarded 返回形状不包含
        timestamp。
        """
        now = self._read_clock()
        discarded = self._discard_expired_events_at(now)
        return Result(events_removed=len(discarded), discarded=discarded)

    def _cleanup_all_at(self, now):
        # 在指定时刻一次性清理全部过期状态：值记录、去重记录与带 TTL 事件
        # 共用同一判定时刻与 <= 边界；三类清理互不干扰（清事件不动 seen，
        # 清值不动队列），剩余事件保持原 FIFO 相对顺序。
        values_removed, dedupe_removed = self._cleanup_at(now)
        events_removed = self._cleanup_events_at(now)
        return values_removed, dedupe_removed, events_removed

    def cleanup_all_expired(self):
        """单次读取时钟，一次性清理该时刻全部过期状态。

        在同一观察点上分别执行：移除 expires_at <= 当前时刻的值记录、到期
        点 <= 当前时刻的去重记录，以及带 event_ttl 且到期点 <= 当前时刻的
        队列事件。事件无论位于队首、中间还是队尾都按原 FIFO 位置移除，剩余
        事件相对顺序不变，释放的槽位可被后续 push 使用；清理事件不删除或
        延长对应 seen 去重窗口，清理值与去重记录不触碰队列。没有可清理项
        时返回三个零，仍只读取一次时钟；时钟抛出的异常原样传播且状态保持
        不变。返回 Result(values_removed=, dedupe_removed=, events_removed=)，
        计数反映本次调用实际删除的记录，重复调用得到零计数。被清理的带 TTL
        事件同样以该观察时刻追加 reason='event_ttl' 的丢弃审计历史，值记录
        与去重记录的清理不写历史。
        """
        now = self._read_clock()
        values_removed, dedupe_removed, events_removed = self._cleanup_all_at(now)
        return Result(
            values_removed=values_removed,
            dedupe_removed=dedupe_removed,
            events_removed=events_removed,
        )

    def _try_push_at(self, dedupe, event, window, now, event_ttl=None):
        # 在指定时钟时刻判定一次入队：window/event_ttl 由调用方先行校验。
        # 返回 (reason, evicted, receipt)：reason 为 None 表示接受，此时
        # receipt 为本次接受新分配的回执（从 1 起递增、不复用，拒绝、出队与
        # 取消都不消耗号）；reason 非 None 时 receipt 为 None 且不分配回执。
        # evicted 为 drop_oldest 策略下被挤出队首的 (event, receipt) 二元组
        # 列表（被挤出事件的回执随事件出列、同样不复用），未挤出时为空列表。
        expiry = self.seen.get(dedupe)
        if expiry is not None and expiry > now:
            # 去重窗口优先：窗口内请求一律报 dedupe_window，不为腾位挤出事件
            return 'dedupe_window', [], None
        evicted = []
        if self.max_queue is not None and len(self.events) >= self.max_queue:
            if self.overflow_policy == 'drop_oldest' and self.max_queue > 0:
                # 挤出 FIFO 队首并同步移除其 event_ttl / 回执 / 去重元数据；
                # 被挤出事件的去重记录保留到原窗口截止，不在此删除
                evicted_event = self.events.popleft()
                self.event_expiries.popleft()
                evicted_receipt = self.event_receipts.popleft()
                self.event_metadata.popleft()
                # 审计历史固定 reason='queue_full'、形状不含回执，时间戳取
                # 触发本次入队判定的观察时刻 now
                evicted.append((evicted_event, evicted_receipt))
                self._record_discard(evicted_event, 'queue_full', now)
            else:
                # 去重已可用但队列已满：拒绝且不登记新的去重占用，也不分配回执；
                # max_queue 为零时 drop_oldest 同样拒绝且不丢弃任何项目
                return 'queue_full', [], None
        # 记录不存在或到期点小于等于当前时刻：允许重新入队
        self.seen[dedupe] = now + window
        self.events.append(event)
        # 事件到期时刻 = 接受时刻 + event_ttl；event_ttl 为零即接受时已到期
        self.event_expiries.append(None if event_ttl is None else now + event_ttl)
        # 回执只在入队被接受的此刻分配一次，回执对齐信息与事件同步入队
        receipt = self._next_receipt
        self._next_receipt += 1
        self.event_receipts.append(receipt)
        # 去重元数据与 seen 记录同一到期点：接收时刻 + window；dedupe 为
        # None 时记的是真实的 None 键（dedupe_known 仍为 True）
        self.event_metadata.append(Result(
            dedupe_known=True, dedupe_key=dedupe,
            dedupe_expires_at=now + window))
        return None, evicted, receipt

    def _push_result(self, reason, evicted):
        # 默认策略保持既有结果形状（仅 accepted/reason）；非默认策略附加
        # discarded 列表，未挤出时为空列表。旧入口的 discarded 条目保持
        # event/reason 两字段形状，不含回执。
        result = Result(accepted=reason is None, reason=reason)
        if self.overflow_policy != 'reject_new':
            result['discarded'] = [
                Result(event=evicted_event, reason='queue_full')
                for evicted_event, _evicted_receipt in evicted]
        return result

    def _push_receipt_result(self, reason, evicted, receipt):
        # 回执入口的结果形状：receipt/accepted/reason；拒绝时 receipt 为 None。
        # 非默认策略附加 discarded，每项为带驱逐回执的
        # Result(event, reason='queue_full', receipt)；旧事件（无回执）被挤出
        # 时驱逐回执为 None。
        result = Result(receipt=receipt, accepted=reason is None, reason=reason)
        if self.overflow_policy != 'reject_new':
            result['discarded'] = [
                Result(event=evicted_event, reason='queue_full',
                       receipt=evicted_receipt)
                for evicted_event, evicted_receipt in evicted]
        return result

    def _try_push(self, dedupe, event, window):
        # 校验失败时不读取时钟，也不产生事件或去重记录
        _check_duration(window, 'window')
        now = self._read_clock()
        return self._try_push_at(dedupe, event, window, now)

    def push(self, dedupe, event, window):
        reason, _evicted, _receipt = self._try_push(dedupe, event, window)
        return reason is None

    def push_with_reason(self, dedupe, event, window):
        reason, evicted, _receipt = self._try_push(dedupe, event, window)
        return self._push_result(reason, evicted)

    def push_with_receipt(self, dedupe, event, window):
        """入队并返回回执的推送，时间与去重边界与 push 完全一致。

        单次读取注入时钟，去重窗口（到期点 > 当前时刻即拦截）与容量判定的
        优先级、FIFO 顺序及 drop_oldest 挤出语义都与 push_with_reason 相同。
        唯一区别是入队被接受时分配一个从 1 起递增、永不复用的正整数回执：
        返回 Result(receipt=回执, accepted=True, reason=None)；被去重窗口或
        满队列拒绝时回执尚未分配，返回
        Result(receipt=None, accepted=False, reason=原因)，reason 只能是
        'dedupe_window' 或 'queue_full'。drop_oldest 挤出队首时附加 discarded
        列表（与 push_with_reason 同一出现条件），每项为
        Result(event=原队首事件, reason='queue_full', receipt=被挤出事件的回执)，
        被挤出的是无回执旧事件时 receipt 为 None。window 非法时抛出
        ValueError 且不读取时钟、不改变状态（含不消耗回执号）。
        """
        reason, evicted, receipt = self._try_push(dedupe, event, window)
        return self._push_receipt_result(reason, evicted, receipt)

    def push_expiring(self, dedupe, event, window, event_ttl):
        reason, _evicted, _receipt = self._try_push_expiring(
            dedupe, event, window, event_ttl)
        return reason is None

    def push_expiring_with_reason(self, dedupe, event, window, event_ttl):
        reason, evicted, _receipt = self._try_push_expiring(
            dedupe, event, window, event_ttl)
        return self._push_result(reason, evicted)

    def push_expiring_with_receipt(self, dedupe, event, window, event_ttl):
        """带事件 TTL 与回执的推送，语义与 push_with_receipt 一致。

        event_ttl 为必选的非负有限时长，事件绝对到期点取接受时刻 +
        event_ttl；其余回执、去重、容量与 discarded 形状约定同
        push_with_receipt。window/event_ttl 非法时抛出 ValueError 且不读取
        时钟、不改变状态（含不消耗回执号）。
        """
        reason, evicted, receipt = self._try_push_expiring(
            dedupe, event, window, event_ttl)
        return self._push_receipt_result(reason, evicted, receipt)

    def _try_push_expiring(self, dedupe, event, window, event_ttl):
        # event_ttl 为必选时长：None 等非法值同样在校验阶段抛出 ValueError，
        # 校验失败时不读取时钟，也不产生事件或去重记录
        _check_duration(window, 'window')
        _check_duration(event_ttl, 'event_ttl')
        now = self._read_clock()
        return self._try_push_at(dedupe, event, window, now, event_ttl)

    def release_dedupe(self, dedupe):
        """提前结束某个去重键的窗口占用，供业务撤销或重新编排时使用。

        dedupe 不可哈希时原样抛出 TypeError，此时尚未读取时钟也未改变任何
        状态。键在 seen 中有去重占用记录时，无论其绝对到期点是否已经过去，
        都删除该记录并返回 Result(released=True, reason=None)；键不存在时
        返回 Result(released=False, reason='missing')。

        纯去重表操作：不读取注入时钟，不触碰 values、events、event_expiries、
        回执、队列容量与 discard_history，已排队事件继续按原 FIFO 顺序保留，
        其回执仍可被消费或取消，去重元数据中的 dedupe_expires_at 也保持
        入队时的记录不变。释放成功后同一去重键可以立即再次 push（新窗口从
        新的接受时刻起算），新旧事件允许同时存在于队列中。
        """
        hash(dedupe)  # 不可哈希时原样抛出 TypeError
        if dedupe in self.seen:
            del self.seen[dedupe]
            return Result(released=True, reason=None)
        return Result(released=False, reason='missing')

    def push_batch(self, batch):
        # 先完整校验批次结构、每项 window/event_ttl 及 dedupe 可哈希性：在此之前不读取时钟、不改变任何状态
        entries = _parse_batch(batch)
        results = []
        if entries:
            # 整批使用同一时钟时刻，时间源只读取一次
            now = self._read_clock()
            for dedupe, event, window, event_ttl in entries:
                # 前项已立即更新 seen 与队列占用，后项据此继续判定；旧批量入口
                # 的结果形状不变，但入队接受同样在内部分配回执（回执不在结果中
                # 返回，可经 pop_with_receipt/peek_with_receipt 观察）
                reason, evicted, _receipt = \
                    self._try_push_at(dedupe, event, window, now, event_ttl)
                results.append(self._push_result(reason, evicted))
        return results

    def apply_batch(self, operations):
        # 先完整校验批次结构、标签、时长及键：在此之前不读取时钟、不改变任何状态
        parsed = _parse_apply_batch(operations)
        results = []
        if parsed:
            # 整批使用同一时钟时刻，时间源只读取一次；时钟抛出的异常原样转出
            now = self._read_clock()
            for op in parsed:
                tag = op[0]
                if tag == 'put':
                    _, key, value, ttl = op
                    self._put_at(key, value, ttl, now)
                    results.append(Result(accepted=True, reason=None))
                elif tag == 'renew':
                    _, key, ttl = op
                    # 与整批共享同一时钟读数：缺失与过期判定与单次 renew 一致，
                    # 只影响 values，不触碰 seen、队列、容量、回执与历史
                    results.append(self._renew_at(key, ttl, now))
                elif tag == 'delete':
                    _, key = op
                    results.append(Result(deleted=self.delete(key)))
                elif tag == 'push':
                    _, dedupe, event, window = op
                    # 前序操作（含 cleanup）已立即更新状态，本项据此在同一时刻判定
                    reason, evicted, _receipt = \
                        self._try_push_at(dedupe, event, window, now)
                    results.append(self._push_result(reason, evicted))
                elif tag == 'push_expiring':
                    _, dedupe, event, window, event_ttl = op
                    reason, evicted, _receipt = \
                        self._try_push_at(dedupe, event, window, now, event_ttl)
                    results.append(self._push_result(reason, evicted))
                elif tag == 'push_with_receipt':
                    _, dedupe, event, window = op
                    # 回执入口与普通 push 同一时刻、同一判定，仅结果形状不同
                    reason, evicted, receipt = \
                        self._try_push_at(dedupe, event, window, now)
                    results.append(self._push_receipt_result(reason, evicted, receipt))
                elif tag == 'push_expiring_with_receipt':
                    _, dedupe, event, window, event_ttl = op
                    reason, evicted, receipt = \
                        self._try_push_at(dedupe, event, window, now, event_ttl)
                    results.append(self._push_receipt_result(reason, evicted, receipt))
                elif tag == 'cancel':
                    _, receipt = op
                    # 取消不读取时钟、不判 event_ttl：与整批观察时刻无关
                    results.append(self.cancel(receipt))
                elif tag == 'release_dedupe':
                    _, dedupe = op
                    # 释放不读取时钟、不按时间判定：与整批观察时刻无关，
                    # 前一次释放立即影响后续 push 或 release 的判定
                    results.append(self.release_dedupe(dedupe))
                elif tag == 'pop_with_receipt':
                    results.append(self.pop_with_receipt())
                elif tag == 'peek_with_receipt':
                    results.append(self.peek_with_receipt())
                elif tag == 'inspect':
                    # 复用整批唯一一次时钟读数，反映此前操作且不改变任何状态，
                    # 因此不影响同批后续操作的判定
                    results.append(self._inspect_at(now))
                elif tag == 'cleanup':
                    values_removed, dedupe_removed = self._cleanup_at(now)
                    results.append(Result(
                        values_removed=values_removed,
                        dedupe_removed=dedupe_removed,
                    ))
                elif tag == 'cleanup_expired_events':
                    # 与整批共享同一时钟读数：到期点 <= 统一观察时刻的带 TTL
                    # 事件全部移除并立即释放槽位，values、seen、max_queue 不变
                    results.append(Result(events_removed=self._cleanup_events_at(now)))
                elif tag == 'discard_expired_events':
                    # 与整批共享同一时钟读数，按操作顺序影响后续操作
                    discarded = self._discard_expired_events_at(now)
                    results.append(Result(
                        events_removed=len(discarded),
                        discarded=discarded,
                    ))
                elif tag == 'resize_queue':
                    _, max_queue, policy = op
                    # 与整批共享同一时钟读数：挤出丢弃的历史时间戳取批次
                    # 观察时刻；新容量与策略立即生效，后续操作据此判定
                    results.append(self._resize_queue_at(max_queue, policy, now))
                else:  # 'cleanup_all_expired'
                    # 与整批共享同一时钟读数，按操作顺序影响后续操作
                    values_removed, dedupe_removed, events_removed = \
                        self._cleanup_all_at(now)
                    results.append(Result(
                        values_removed=values_removed,
                        dedupe_removed=dedupe_removed,
                        events_removed=events_removed,
                    ))
        return results

    def replay_batch(self, records):
        """按记录自带的逻辑时间戳确定性回放一批操作。

        每项记录为 (timestamp, operation)：timestamp 是非 bool 的有限
        int/float 且按非递减顺序出现；operation 除 apply_batch 的
        put/renew/delete/push/push_expiring/push_with_receipt/
        push_expiring_with_receipt/release_dedupe/cancel/inspect/cleanup/
        cleanup_all_expired/discard_expired_events/resize_queue 与
        ('cleanup_expired_events',) 外，还可表达读取与出队路径：
        ('get', key)、('get_with_reason', key)、('pop',)、
        ('pop_with_receipt',)、('pop_batch'[, limit])、('peek'[, limit])、
        ('peek_with_receipt',)、('pop_live_batch'[, limit])、
        ('peek_live_batch'[, limit]) 与 ('queue_status',)。每条记录以自己的
        timestamp 作为当前时刻计算 TTL、event_ttl 与去重窗口的绝对边界，同一
        时间戳共享该边界；回执随入队接受顺序在回放实例上继续递增，拒绝不分配
        回执；cancel、release_dedupe、pop_with_receipt 与 peek_with_receipt
        与对应公开入口一样不读取任何时钟（cancel 也不按 event_ttl 判定）；回放全程不读取注入
        时钟、不启动后台线程，记录时间的推进本身不触发 values/seen/事件的任何
        自动清理。溢出挤出（drop_oldest 的 queue_full 与 resize_queue 缩容
        挤出）、事件 TTL 清理（cleanup_expired_events、
        discard_expired_events、cleanup_all_expired）与过期感知出队
        （pop_live_batch）产生的丢弃审计历史与实时调用完全一致：按操作
        顺序追加，timestamp 取各记录自带时间戳而非注入时钟；普通
        pop/pop_batch、reject_new 的 queue_full 拒绝与 dedupe_window
        拒绝同样不写历史。

        读取与出队记录的时间语义与对应公开入口一致：get 按记录时刻判定
        键值 TTL，到期点 <= 记录时刻时返回 None 并移除该键（键不存在同样
        返回 None）；get_with_reason 同样按记录时刻判定且不读取注入时钟，
        按 found/value/reason 三字段返回 missing/expired/有效三种结果，过期
        键被移除后对后续记录表现为 missing；pop 与 pop_batch 不读取时间，
        即使事件已到期也按 FIFO 原样取出，空队列分别返回 None 与 []；peek 只观察前缀、queue_status
        只报告 size/max_queue，二者都不改变任何状态；pop_live_batch 与
        peek_live_batch 按记录时刻扫描，无 event_ttl 的事件始终有效，带
        TTL 且到期点 <= 记录时刻的事件按原 FIFO 扫描顺序放入 discarded
        （每项为 Result(event=原事件值, reason='event_ttl')）：前者移除已
        扫描项目，limit 为正整数时交付够 limit 个有效事件即停止，未扫描
        尾部（含其中恰好到期的事件）原样留队并继续占用槽位；后者只观察、
        不移除任何项目，peek 报告的过期项不释放队列槽位。limit 为 None 时
        扫描/取出整个队列，为 0 时两者都返回两个空列表。读取记录的返回值
        可被后续记录继续消费：前序 pop/pop_batch/pop_live_batch 已移除的
        项目不会再出现，前序 get/get_with_reason 已移除的过期键对后续读取
        表现为不存在。

        先完整校验全部记录再改动状态：结构、标签、时间戳单调递增与
        limit（None 或非 bool 的非负整数）全部合法后才执行，任一记录非法
        时整批拒绝，缓存保持原样；key/dedupe 不可哈希时原样抛出
        TypeError。空记录返回空列表且不读取时钟。成功时返回与输入逐项
        对应、形状与各公开操作一致的结果列表：put 为 accepted/reason，
        renew 为 renewed/value/reason/expires_at（与单次 renew 同形状，
        到期点按记录自带时间戳计算，可确定性重放），
        push 类为 accepted 与 reason（None/dedupe_window/queue_full），
        非默认 overflow_policy 下与 push_with_reason 一样附加 discarded
        列表（被挤出队首的事件记录，未挤出时为空），
        delete 为 deleted，cleanup 为 values_removed/dedupe_removed，
        cleanup_expired_events 为 events_removed，discard_expired_events
        为 events_removed/discarded（形状与公开方法一致），
        resize_queue 为 max_queue/overflow_policy/discarded（形状与
        公开 resize_queue 一致，后续记录按新容量与策略判定），
        cleanup_all_expired 为
        values_removed/dedupe_removed/events_removed，get/pop 为单个值，
        get_with_reason 为含 found/value/reason 三个字段的 Result，
        push 类回执入口为 receipt/accepted/reason（非默认策略再带 discarded，
        驱逐项含 receipt），cancel 为 removed/event/reason，
        release_dedupe 为 released/reason（释放不读取任何时钟，记录时间的
        推进本身不触发去重记录的自动清理），
        pop_with_receipt 为含 found/event/receipt 三个字段的 Result（空队列
        found=False、event=None、receipt=None），
        pop_batch/peek 为普通 list，peek_with_receipt 为含
        Result(event, receipt) 项的普通 list（空队列为空列表），
        pop_live_batch/peek_live_batch 为含
        events/discarded 两个 list 的 Result，queue_status 为含
        size/max_queue 的 Result，inspect 为含
        observed_at/value_count/expired_value_count/dedupe_count/
        expired_dedupe_count/queue_size/live_event_count/expired_event_count/
        next_value_expiry/next_dedupe_expiry/next_event_expiry/
        discard_history_size 十二个字段的 Result（observed_at 取记录自带
        timestamp，同一记录序列在同构初始状态上得到相同结果；查询不改变任何
        状态，故不影响同批后续记录）。回放写入的绝对到期时间与常规路径一致，
        可由 snapshot 保存并由 restore 恢复。
        """
        # 先完整校验记录结构、时间戳单调性、操作、limit 与键：在此之前不读取时钟、不改变任何状态
        parsed = _parse_replay_batch(records)
        results = []
        for now, op in parsed:
            tag = op[0]
            if tag == 'put':
                _, key, value, ttl = op
                self._put_at(key, value, ttl, now)
                results.append(Result(accepted=True, reason=None))
            elif tag == 'renew':
                _, key, ttl = op
                # 以记录自带时刻为观察点计算新到期点，不读取注入时钟；
                # missing/expired/续期成功三种结果与单次 renew 字段一致
                results.append(self._renew_at(key, ttl, now))
            elif tag == 'delete':
                _, key = op
                # delete 本身不读取时钟，语义与单项/批量入口一致
                results.append(Result(deleted=self.delete(key)))
            elif tag == 'push':
                _, dedupe, event, window = op
                # 前序记录已立即更新状态，本记录按其自带时刻判定
                reason, evicted, _receipt = \
                    self._try_push_at(dedupe, event, window, now)
                results.append(self._push_result(reason, evicted))
            elif tag == 'push_expiring':
                _, dedupe, event, window, event_ttl = op
                reason, evicted, _receipt = \
                    self._try_push_at(dedupe, event, window, now, event_ttl)
                results.append(self._push_result(reason, evicted))
            elif tag == 'push_with_receipt':
                _, dedupe, event, window = op
                # 回执按入队接受顺序在回放实例上继续递增；TTL/去重按记录时刻
                reason, evicted, receipt = \
                    self._try_push_at(dedupe, event, window, now)
                results.append(self._push_receipt_result(reason, evicted, receipt))
            elif tag == 'push_expiring_with_receipt':
                _, dedupe, event, window, event_ttl = op
                reason, evicted, receipt = \
                    self._try_push_at(dedupe, event, window, now, event_ttl)
                results.append(self._push_receipt_result(reason, evicted, receipt))
            elif tag == 'cancel':
                _, receipt = op
                # 取消不读取任何时钟（含记录时刻）：不按 event_ttl 判定
                results.append(self.cancel(receipt))
            elif tag == 'release_dedupe':
                _, dedupe = op
                # 释放不读取任何时钟（含记录时刻）：只按 seen 当前占用判定，
                # 记录时间的推进本身不触发去重记录的任何自动清理
                results.append(self.release_dedupe(dedupe))
            elif tag == 'cleanup':
                values_removed, dedupe_removed = self._cleanup_at(now)
                results.append(Result(
                    values_removed=values_removed,
                    dedupe_removed=dedupe_removed,
                ))
            elif tag == 'cleanup_expired_events':
                results.append(Result(events_removed=self._cleanup_events_at(now)))
            elif tag == 'discard_expired_events':
                # 以记录自带时刻为观察点，不读取注入时钟
                discarded = self._discard_expired_events_at(now)
                results.append(Result(
                    events_removed=len(discarded),
                    discarded=discarded,
                ))
            elif tag == 'resize_queue':
                _, max_queue, policy = op
                # 以记录自带时刻为观察点写入挤出丢弃历史，不读取注入时钟；
                # 新容量与策略对后续记录立即生效
                results.append(self._resize_queue_at(max_queue, policy, now))
            elif tag == 'cleanup_all_expired':
                # 以记录自带时刻为观察点，不读取注入时钟
                values_removed, dedupe_removed, events_removed = \
                    self._cleanup_all_at(now)
                results.append(Result(
                    values_removed=values_removed,
                    dedupe_removed=dedupe_removed,
                    events_removed=events_removed,
                ))
            elif tag == 'get':
                _, key = op
                # 按记录时刻判定键值 TTL：到期点 <= 记录时刻即移除并返回 None
                results.append(self._get_at(key, now))
            elif tag == 'get_with_reason':
                _, key = op
                # 按记录时刻判定并区分 missing/expired/有效；缺失不读时钟的
                # 约定在此表现为直接返回 missing，全程不读取注入时钟
                if key not in self.values:
                    results.append(Result(found=False, value=None, reason='missing'))
                else:
                    results.append(self._get_with_reason_at(key, now))
            elif tag == 'pop':
                # pop 不读取时间：过期事件同样按 FIFO 原样取出，空队列返回 None
                results.append(self.pop())
            elif tag == 'pop_with_receipt':
                # 与 pop 同属不读时间的出队路径，只是同时回报回执
                results.append(self.pop_with_receipt())
            elif tag == 'pop_batch':
                _, limit = op
                # pop_batch 不读取时间，过期事件也原样取出
                results.append(self.pop_batch(limit))
            elif tag == 'peek':
                _, limit = op
                # 纯观察：不移除任何项目、不释放槽位
                results.append(self.peek(limit))
            elif tag == 'peek_with_receipt':
                # 与 peek 同为只读、不读时间，项为含 event/receipt 的 Result
                results.append(self.peek_with_receipt())
            elif tag == 'inspect':
                # 只读诊断：以记录自带 timestamp 作为 observed_at，全程不
                # 读取注入时钟；不改变任何状态，因此不影响同批后续记录
                results.append(self._inspect_at(now))
            elif tag == 'pop_live_batch':
                _, limit = op
                # 按记录时刻判定事件 TTL；已扫描项目出队，未扫描尾部原样保留
                results.append(self._pop_live_batch_at(limit, now))
            elif tag == 'peek_live_batch':
                _, limit = op
                # 同样的 TTL 判定但只读：报告的过期项不视为已释放的槽位
                results.append(self._peek_live_batch_at(limit, now))
            else:  # 'queue_status'
                results.append(self.queue_status())
        return results

    def pop(self):
        if not self.events:
            return None
        self.event_expiries.popleft()
        self.event_receipts.popleft()
        self.event_metadata.popleft()
        return self.events.popleft()

    def pop_with_receipt(self):
        """按 FIFO 取出队首事件并同时返回其回执。

        与 pop 同一出队路径：不读取时钟、不做 event_ttl 判定，已到期事件也
        原样取出。命中时返回 Result(found=True, event=原事件值,
        receipt=该事件回执)，事件值为 None 时仍以 found=True 原样返回；空队列
        返回 Result(found=False, event=None, receipt=None)。取出后该回执不再
        可用于 cancel（按 missing 报告），且永不复用。纯出队：只释放一个队列
        容量位置，不触碰 values/seen，不写丢弃审计历史。
        """
        if not self.events:
            return Result(found=False, event=None, receipt=None)
        self.event_expiries.popleft()
        receipt = self.event_receipts.popleft()
        self.event_metadata.popleft()
        event = self.events.popleft()
        return Result(found=True, event=event, receipt=receipt)

    def pop_batch(self, limit=None):
        """按 FIFO 从队头批量取出事件。

        limit 为 None（缺省）时取出当前队列全部事件；为非负整数时最多取出
        该数量，数量不足只返回实际存在的事件。纯出队操作：不读取时钟、不触发
        过期清理、不改变 values/seen/max_queue，每个被取出的事件只释放一个
        队列容量位置。limit 为负数、浮点数、字符串、布尔值或其他非整数时
        抛出 ValueError，且不移除任何事件。
        """
        _check_limit(limit)
        count = len(self.events) if limit is None else min(limit, len(self.events))
        # 逐个 popleft 与连续调用 pop 的顺序和元素完全一致，事件为 None 也原样保留
        taken = []
        for _ in range(count):
            taken.append(self.events.popleft())
            self.event_expiries.popleft()
            # 回执随事件一并出列且不复用；pop_batch 的返回形状保持只有事件
            self.event_receipts.popleft()
            self.event_metadata.popleft()
        return taken

    def peek(self, limit=None):
        """非破坏性地查看队头事件：按插入顺序返回前缀，不移除任何事件。

        limit 为 None（缺省）时返回当前队列的全部内容；为非负整数时最多返回
        该数量的队头前缀，数量不足只返回实际存在的事件。纯查看操作：不读取
        时钟、不触发过期清理、不触碰 event_expiries/values/seen/max_queue，
        队列中的 None 与其他对象按原引用返回。空队列与 limit 为零同样不读取
        时钟。limit 为负数、浮点数、字符串、布尔值或其他非整数时抛出
        ValueError，抛出前不改变任何状态。
        """
        _check_limit(limit)
        count = len(self.events) if limit is None else min(limit, len(self.events))
        # 只物化队头前缀副本，deque 本身保持不变
        return [self.events[i] for i in range(count)]

    def peek_with_receipt(self):
        """非破坏性地查看整个 FIFO 队列，每项同时给出事件与其回执。

        与 peek()（不带 limit）同一纯查看路径：不读取时钟、不触发过期清理、
        不移除任何项目、不释放槽位、不触碰 values/seen。按当前 FIFO 顺序返回
        普通 list，每项为 Result(event=事件, receipt=该事件回执)，事件为 None
        也作为一项保留；无回执的旧事件（如旧快照恢复出的事件）其 receipt 为
        None。空队列返回空列表 []。返回的列表为物化副本，与内部 deque 分离。
        """
        # 只物化 FIFO 全量副本；事件与回执按对齐位置逐项配对
        return [Result(event=event, receipt=receipt)
                for event, receipt in zip(self.events, self.event_receipts)]

    @staticmethod
    def _metadata_result(event, expiry, receipt, metadata):
        # peek_with_metadata/pop_with_metadata 的单项结果形状：事件本体、
        # 去重键与其窗口到期点、事件 TTL 到期点、回执与 known 标记。
        # 未知键旧条目的 dedupe_key/dedupe_expires_at 已为 None。
        return Result(
            event=event,
            dedupe_key=metadata.dedupe_key,
            dedupe_expires_at=metadata.dedupe_expires_at,
            event_expires_at=expiry,
            receipt=receipt,
            dedupe_known=metadata.dedupe_known,
        )

    def peek_with_metadata(self, limit=None):
        """非破坏性地查看队头事件及其去重元数据：按插入顺序返回前缀。

        limit 为 None（缺省）时返回当前队列的全部条目；为非负整数时最多返回
        该数量的队头前缀，数量不足只返回实际存在的条目。按当前 FIFO 顺序返回
        普通 list，每项为 Result(event=事件, dedupe_key=去重键,
        dedupe_expires_at=去重窗口绝对到期点, event_expires_at=事件 TTL 绝对
        到期点或 None, receipt=回执或 None, dedupe_known=去重键是否已知)。
        经 push 入口接受的事件 dedupe_known 为 True（dedupe 为 None 时记的是
        真实的 None 键），dedupe_expires_at 为接收时刻加 window；旧快照恢复
        的未知键条目 dedupe_known 为 False，dedupe_key 与 dedupe_expires_at
        均为 None。

        纯查看操作：不读取时钟、不因条目过期而清理或写入丢弃历史、不移除任何
        项目、不释放槽位、不触碰 values/seen，过期事件也原样报告。空队列与
        limit 为零返回空列表。limit 为负数、浮点数、字符串、布尔值或其他非
        整数时抛出 ValueError，抛出前不读取时钟、不改变任何状态。
        """
        _check_limit(limit)
        count = len(self.events) if limit is None else min(limit, len(self.events))
        # 只物化队头前缀副本，各对齐 deque 本身保持不变
        return [self._metadata_result(
                    self.events[i], self.event_expiries[i],
                    self.event_receipts[i], self.event_metadata[i])
                for i in range(count)]

    def pop_with_metadata(self, limit=None):
        """按 FIFO 从队头批量取出事件及其去重元数据。

        limit 为 None（缺省）时取出当前队列全部条目；为非负整数时最多取出
        该数量，数量不足只返回实际存在的条目。返回普通 list，每项形状与
        peek_with_metadata 相同：Result(event, dedupe_key, dedupe_expires_at,
        event_expires_at, receipt, dedupe_known)，顺序与当前队列一致。

        纯出队操作：不读取时钟、不触发过期清理、不写入丢弃历史，只移除返回的
        队列项并为每项释放一个队列容量位置；不触碰 values，也不删除 seen 中
        对应的去重窗口（窗口保留到原截止时刻，窗口内同去重键仍被去重拦截）。
        被取出事件的回执随事件出列且不复用。limit 为负数、浮点数、字符串、
        布尔值或其他非整数时抛出 ValueError，抛出前不读取时钟、不改变任何
        状态。
        """
        _check_limit(limit)
        count = len(self.events) if limit is None else min(limit, len(self.events))
        # 逐个 popleft 与连续调用 pop 的顺序和元素完全一致
        taken = []
        for _ in range(count):
            event = self.events.popleft()
            expiry = self.event_expiries.popleft()
            receipt = self.event_receipts.popleft()
            metadata = self.event_metadata.popleft()
            taken.append(self._metadata_result(event, expiry, receipt, metadata))
        return taken

    def _pop_live_batch_at(self, limit, now):
        # 在指定时刻执行过期感知出队；limit 由调用方先行校验。空队列或
        # limit == 0 时与公开入口一致：不扫描、不改状态，返回两个空列表。
        if limit == 0 or not self.events:
            return Result(events=[], discarded=[])
        events = []
        discarded = []
        # 从队头逐项判定：有效事件与过期事件都已出列，停止扫描后剩余元素
        # 自然保持原序留在 deque 中，槽位不被提前释放
        while self.events:
            event = self.events.popleft()
            expiry = self.event_expiries.popleft()
            # 回执与去重元数据随被扫描项目（无论交付还是丢弃）一并出列，
            # 回执不复用
            self.event_receipts.popleft()
            self.event_metadata.popleft()
            if expiry is not None and expiry <= now:
                # 到期边界与 values/seen/cleanup_expired_events 一致：<= 即过期
                discarded.append(Result(event=event, reason='event_ttl'))
                # 过期感知出队同样属于 event_ttl 清理，写入审计历史
                self._record_discard(event, 'event_ttl', now)
            else:
                events.append(event)
                if limit is not None and len(events) >= limit:
                    break
        return Result(events=events, discarded=discarded)

    def pop_live_batch(self, limit=None):
        """过期感知的批量出队：一次时钟判断同时给出有效事件与被丢弃事件。

        按当前队列的插入顺序从队头扫描：未设置 event_ttl 的事件始终可消费，
        原样放入 events；设置了 event_ttl 且绝对到期点 <= 本次读取时刻的事件
        从队列移除，并在 discarded 中按原顺序追加
        Result(event=原事件值, reason='event_ttl')。过期项位于队头或队中都不
        改变其余事件的相对顺序。整次调用只读取一次注入时钟。

        limit 为 None（缺省）时处理整个队列；为正整数时在取到该数量的有效
        事件后立即停止扫描，其后的事件（含恰好已到期者）一律不检查、不出队，
        仍占用原 max_queue 槽位；为 0 时不读取时钟也不改变状态，直接返回两个
        空列表。队列为空时同样不读取时钟。被移除的过期事件与返回的有效事件
        一样释放队列槽位，但两者都不触碰 values 与 seen：过期事件的去重记录
        保留到原去重窗口截止，下一次相同去重键继续遵循既有判定。每个被移除
        的过期事件还按扫描顺序以本次时钟读数为 timestamp 追加一条
        reason='event_ttl' 的丢弃审计历史；返回的 discarded 条目仍只有
        event/reason 两字段。

        limit 不是 None 且不是非 bool 的非负整数（负数、浮点数、字符串、
        布尔值等）时抛出 ValueError，抛出前不读取时钟、不改变任何状态；时钟
        抛出的异常原样传播。返回 Result(events=有效事件列表,
        discarded=丢弃结果列表)，两者均为普通 list。
        """
        _check_limit(limit)
        if limit == 0 or not self.events:
            # 显式零配额或空队列：不读时钟、不扫描、不改状态
            return Result(events=[], discarded=[])
        now = self._read_clock()
        return self._pop_live_batch_at(limit, now)

    def _peek_live_batch_at(self, limit, now):
        # 在指定时刻只读扫描，语义与 _pop_live_batch_at 完全一致但不出队；
        # limit 由调用方先行校验。空队列或 limit == 0 时同样直接返回空结果。
        if limit == 0 or not self.events:
            return Result(events=[], discarded=[])
        events = []
        discarded = []
        # 只读扫描：索引遍历而不 popleft，队列与 event_expiries 原样保留
        for event, expiry in zip(self.events, self.event_expiries):
            if expiry is not None and expiry <= now:
                # 到期边界与 pop_live_batch 一致：<= 即过期
                discarded.append(Result(event=event, reason='event_ttl'))
            else:
                events.append(event)
                if limit is not None and len(events) >= limit:
                    break
        return Result(events=events, discarded=discarded)

    def peek_live_batch(self, limit=None):
        """非破坏性地预览 pop_live_batch：同样的 TTL 判定与边界，但不移除事件。

        按当前队列的插入顺序从队头扫描：未设置 event_ttl 的事件始终有效，
        原样放入 events；设置了 event_ttl 且绝对到期点 <= 本次读取时刻的事件
        归入 discarded，按原顺序追加 Result(event=原事件值, reason='event_ttl')。
        与 pop_live_batch 的唯一区别是不出队：任何情况下都不移除事件、不释放
        容量、不清理 values 或 seen，未扫描的尾部保持队列原状。

        limit 为 None（缺省）时扫描整个队列；为正整数时在预览到该数量的有效
        事件后立即停止扫描，其后的事件（含恰好已到期者）一律不检查；为 0 时
        不读取时钟也不改变状态，直接返回两个空列表。队列为空时同样不读取
        时钟；非空且需要扫描时整次调用只读取一次注入时钟，时钟抛出的异常
        原样传播且状态保持完整。

        limit 不是 None 且不是非 bool 的非负整数（负数、浮点数、字符串、
        布尔值等）时抛出 ValueError，抛出前不读取时钟、不改变任何状态。
        返回 Result(events=有效事件列表, discarded=丢弃结果列表)，两者均为
        普通 list。
        """
        _check_limit(limit)
        if limit == 0 or not self.events:
            # 显式零配额或空队列：不读时钟、不扫描、不改状态
            return Result(events=[], discarded=[])
        now = self._read_clock()
        return self._peek_live_batch_at(limit, now)

    def queue_status(self):
        # 纯查询：不读取时钟、不触发清理、不改变队列
        return Result(size=len(self.events), max_queue=self.max_queue)

    def cancel(self, receipt):
        """按回执移除仍在队列中的事件，不读取时钟、不判 event_ttl。

        receipt 必须是排除 bool 的正整数，否则抛出 ValueError 且不改变任何
        状态。命中时从 FIFO 队列移除该事件（其余事件相对顺序不变）并立即释放
        一个容量位置，返回 Result(removed=True, event=原事件值, reason=None)：
        即使事件早已超过其 event_ttl，只要它仍在队列中就照样取消（取消不做
        TTL 判定）。未知回执、对应事件已出队（pop/pop_batch/
        pop_live_batch）、已被驱逐或已被取消时，返回
        Result(removed=False, event=None, reason='missing')。

        取消是纯队列移除：不读取注入时钟，不触碰 values 与该事件的 seen 去重
        窗口（窗口保留到原截止时刻，因此窗口内同去重键仍会被去重拦截），不写
        入 discard_history（丢弃审计只记 event_ttl 与 queue_full），也不复用
        回执号。无回执的旧事件（receipt 为 None）不可能被命中。
        """
        # 回执非法（含 bool、0、负数、浮点数、字符串等）统一 ValueError，
        # 在校验阶段抛出，不读取时钟也不触碰任何状态
        _check_receipt(receipt)
        index = None
        for i, queued in enumerate(self.event_receipts):
            if queued == receipt:
                index = i
                break
        if index is None:
            # 未知、已出队、已驱逐或已取消：回执永不复用
            return Result(removed=False, event=None, reason='missing')
        # 命中：在同一索引处移除对齐的事件、到期信息、回执与去重元数据。
        # 回执单调且不复用，因此至多命中一个位置；del deque[i] 保持其余元素
        # 的相对顺序。
        event = self.events[index]
        del self.events[index]
        del self.event_expiries[index]
        del self.event_receipts[index]
        del self.event_metadata[index]
        # 仅释放容量：seen 去重窗口保留，不写丢弃历史，不推进回执计数
        return Result(removed=True, event=event, reason=None)

    def _resize_queue_at(self, max_queue, overflow_policy, now):
        # 在指定观察时刻应用新的容量与溢出策略；max_queue/overflow_policy 由
        # 调用方先行校验，overflow_policy 为 None 表示沿用当前策略。now 由
        # 调用方显式提供（实时路径为当时钟读数，回放路径为记录时间戳），
        # 本方法自身绝不读取注入时钟。缩容超出现有队列长度时按 FIFO 队首
        # 挤出至新上限（过期事件照样计数，不做任何过期扫描；max_queue 为零
        # 则全部挤出），被挤出事件同步移除其 event_expiries 元数据但保留
        # 对应 seen 去重记录，并按 now 追加 reason='queue_full' 的审计历史。
        if overflow_policy is None:
            overflow_policy = self.overflow_policy
        self.max_queue = max_queue
        self.overflow_policy = overflow_policy
        discarded = []
        if max_queue is not None:
            while len(self.events) > max_queue:
                evicted = self.events.popleft()
                self.event_expiries.popleft()
                # 回执随被挤出事件一并出列且不复用；resize 的 discarded 形状
                # 保持不变（只有 event/reason），不携带回执
                self.event_receipts.popleft()
                self.event_metadata.popleft()
                # 事件值为 None 也保留该条丢弃记录；审计历史固定
                # reason='queue_full'，时间戳取本次调整的观察时刻 now
                discarded.append(Result(event=evicted, reason='queue_full'))
                self._record_discard(evicted, 'queue_full', now)
        return Result(max_queue=max_queue, overflow_policy=overflow_policy,
                      discarded=discarded)

    def resize_queue(self, max_queue, overflow_policy=_KEEP_POLICY):
        """运行期调整队列容量与溢出策略，不改变既有读写与出队语义。

        max_queue 为 None（无限）或非负整数；overflow_policy 省略时沿用当前
        策略，显式给出时只能是 'reject_new' 或 'drop_oldest'。两个参数在
        读取时钟或改变任何状态前完整校验，非法时抛出 ValueError 且
        values/events/event_expiries/seen、现有容量策略与丢弃历史全部保持
        不变。

        扩容、保持容量或只换策略时不清理任何值、去重记录或事件，也不因已有
        过期项扫描队列，且不读取注入时钟。缩容后的上限低于现有队列长度时，
        按 FIFO 从队首挤出至新上限：过期事件照样计入长度（不做过期判定），
        max_queue 为零则全部挤出；每个被挤出事件按顺序以
        Result(event=原事件值, reason='queue_full') 放入返回的 discarded，
        同步移除其 event_expiries 元数据，但对应 seen 去重记录保留到原窗口
        截止。需要挤出时整次调用只读取一次注入时钟，并以该时刻为每个被挤出
        事件追加 reason='queue_full' 的丢弃审计历史（受
        discard_history_limit 容量约束）；时钟抛出的异常原样传播，且配置、
        队列、到期信息与历史均保持不变。

        返回 Result(max_queue=生效后的上限, overflow_policy=生效后的策略,
        discarded=被挤出事件的丢弃记录列表)，无挤出时 discarded 为空列表。
        新配置立即反映到 queue_status 与 snapshot，后续 push/cleanup/pop
        等操作按新容量与策略判定。
        """
        # 校验失败时不读取时钟，也不改变任何状态
        _check_max_queue(max_queue)
        if overflow_policy is _KEEP_POLICY:
            policy = None  # 沿用当前策略
        else:
            _check_overflow_policy(overflow_policy)
            policy = overflow_policy
        # 仅在确实需要挤出时读取时钟，且整次调用只读一次；时钟异常在
        # 任何状态修改之前抛出，配置与队列保持原样
        if max_queue is not None and len(self.events) > max_queue:
            now = self._read_clock()
        else:
            now = None
        return self._resize_queue_at(max_queue, policy, now)

    def discard_history(self, limit=None):
        """返回丢弃审计历史的独立副本，按丢弃先后排列。

        只记录两类丢弃：事件因 event_ttl 到期被清理
        （cleanup_expired_events、discard_expired_events、
        cleanup_all_expired、apply_batch/replay_batch 中的对应操作以及
        pop_live_batch 的过期感知出队），原因固定为 'event_ttl'；
        drop_oldest 策略下入队从队首挤出事件，或 resize_queue 缩容时按
        FIFO 队首挤出事件，原因固定为 'queue_full'。
        普通 pop/pop_batch 消费、reject_new 下的 queue_full 拒绝与
        dedupe_window 拒绝一律不写历史。每项为
        Result(event=原事件值, reason=原因, timestamp=触发该丢弃动作的那次
        观察时刻)：实时路径取当时钟读数，回放路径取记录自带时间戳；批次
        内按操作顺序追加，事件为 None 同样记录。

        limit 为 None（缺省）时返回全部历史；为非负整数时只返回最近的该
        数量，不足时返回实际存在的记录，零返回空列表。纯查询：不读取
        时钟、不触发清理、不改变队列或历史，返回的列表及其中 Result 均为
        与内部状态分离的独立副本（事件对象按既有接口语义保留引用）。
        limit 为负数、浮点数、字符串、布尔值或其他非整数时抛出
        ValueError，抛出前不读取时钟、不改变任何状态。
        """
        _check_limit(limit)
        history = self._discard_history
        if limit is None:
            start = 0
        else:
            start = len(history) - limit
            if start < 0:
                start = 0
        # 逐项物化为全新 Result，返回列表与内部 deque 相互独立
        return [Result(event=entry.event, reason=entry.reason, timestamp=entry.timestamp)
                for entry in list(history)[start:]]

    def clear_discard_history(self):
        """清空丢弃审计历史并返回被清除的记录数量。

        纯状态操作：不读取时钟、不触发清理、不触碰 values/events/seen/
        max_queue 与历史容量配置；历史为空时返回 0。
        """
        removed = len(self._discard_history)
        self._discard_history.clear()
        return removed

    def snapshot(self):
        """捕获某一时刻的可检查、可恢复状态快照。

        纯查询：不读取时钟、不触发任何惰性或显式清理，快照中的过期 values/seen
        记录与未出队事件一律原样保留。结果始终含 values、events、seen、
        max_queue 以及回执的两个字段：与 events 逐项对齐的 event_receipts
        （每项为该事件的正整数回执；旧快照恢复出的无回执事件为 None）与
        next_receipt（下一个待分配回执，严格大于任一对齐回执；即使队列已空，
        已分配过的号也借由该字段保留，绝不复用）；并始终含与 events 逐项对齐
        的 event_metadata：每项为含 dedupe_known、dedupe_key、
        dedupe_expires_at 的 Result，dedupe_known 为 True 表示去重键已知
        （dedupe_key 为 None 时是真实的 None 键），为 False 表示旧快照恢复
        的未知键条目（后两个字段为 None）。队列中存在带 TTL 事件时
        增加 event_expiries 字段，无 TTL 的事件以 None 表示；overflow_policy
        非默认（'drop_oldest'）时增加同名字段。
        丢弃审计历史非空或历史容量非默认（非 None）时增加 discard_history
        （按丢弃先后排列的条目列表，每项为含 event、reason、timestamp 的
        Result）与 discard_history_limit（None 表示无限，否则为非负整数）
        两个字段；容量有限但历史暂为空（如容量为零）时同样记录容量配置。
        clock_policy 非默认（'reject_regression'）时增加 clock_policy 与
        last_clock_time 两个字段（后者为最近一次成功采样的实时时钟水位，
        首次采样前为 None）；默认的 allow_regression 不写入这两个字段，
        快照字段形状保持兼容。
        外层字典、事件列表、对齐列表与历史列表均为与缓存分离的副本，随后任一
        方增删都不会影响另一方；value、事件对象与历史中的事件对象按既有接口
        语义保留引用。
        """
        snap = Snapshot(
            values=dict(self.values),
            events=list(self.events),
            seen=dict(self.seen),
            max_queue=self.max_queue,
            # 回执状态始终随快照保存：event_receipts 与 events 等长对齐，
            # next_receipt 即便队列为空也保留已分配号段，保证恢复后不复用
            event_receipts=list(self.event_receipts),
            next_receipt=self._next_receipt,
            # 去重元数据同样始终随快照保存并逐项物化为全新 Result，
            # 与内部 deque 相互独立（去重键对象按既有接口语义保留引用）
            event_metadata=[
                Result(dedupe_known=entry.dedupe_known,
                       dedupe_key=entry.dedupe_key,
                       dedupe_expires_at=entry.dedupe_expires_at)
                for entry in self.event_metadata
            ],
        )
        if any(expiry is not None for expiry in self.event_expiries):
            snap['event_expiries'] = list(self.event_expiries)
        if self.overflow_policy != 'reject_new':
            snap['overflow_policy'] = self.overflow_policy
        if self._discard_history or self.discard_history_limit is not None:
            # 逐项物化为全新 Result，历史列表与内部 deque 相互独立
            snap['discard_history'] = [
                Result(event=entry.event, reason=entry.reason, timestamp=entry.timestamp)
                for entry in self._discard_history
            ]
            snap['discard_history_limit'] = self.discard_history_limit
        if self.clock_policy != 'allow_regression':
            # 仅非默认策略写入时钟保护字段：默认策略快照形状保持兼容
            snap['clock_policy'] = self.clock_policy
            snap['last_clock_time'] = self._last_clock_time
        return snap

    def restore(self, snapshot):
        """从快照一次性恢复 values、events、seen、max_queue（及事件到期信息、溢出策略、丢弃历史与回执状态）。

        先完整解析并校验快照：在此之前不读取时钟、不改变任何状态，校验失败时
        原状态、队列顺序、容量、回执计数与丢弃历史完全保持。接受不含
        event_expiries 与 overflow_policy 的旧格式（恢复后所有事件均无 TTL，
        溢出策略按 'reject_new' 解释）、只含其一或两者皆含的新格式；
        discard_history 与 discard_history_limit 两个字段同样可选，缺前者按
        空历史、缺后者按无限容量解释。回执的 event_receipts / next_receipt
        两个字段成对出现、皆可整体省略：缺省时在队事件全部视为无回执旧事件
        （event_receipts 全 None，peek_with_receipt 中 receipt 为 None，
        cancel 不命中），下一个回执从 1 开始分配；给出时 event_receipts 必须与
        events 等长对齐、每项为 None 或排除 bool 的正整数，next_receipt 必须
        是严格大于任一对齐回执的正整数，恢复后在其基础上继续递增、绝不复用。
        event_metadata 字段同样可整体省略：缺省时在队事件全部视为未知键旧
        条目（dedupe_known=False，dedupe_key 与 dedupe_expires_at 为 None，
        peek_with_metadata/pop_with_metadata 因此对这些条目返回 None 字段）；
        给出时必须是与 events 等长对齐的列表，每项为恰好含 dedupe_known/
        dedupe_key/dedupe_expires_at 的映射，known 标记只能是 bool，去重键
        必须可哈希（None 表示真实的 None 键），到期点为 None 或非 bool 的
        有限 int/float；长度不符、known 标记非 bool、到期时间或键类型非法
        时统一抛出 ValueError 且保持原状态。
        历史条目必须是恰好含 event/reason/timestamp 的映射，reason 只能是
        'event_ttl' 或 'queue_full'，timestamp 为非 bool 的有限 int/float，
        历史容量为 None 或非负整数，任一非法都抛出 ValueError 且保持原状态。
        max_queue 为非负整数时事件条目数不得超过该上限（零上限只接受空队列，
        判断针对实际条目数而非过期与否），超容快照整体拒绝并抛出 ValueError，
        不截断、不挤出、不接受后等待后续写入处理。时钟保护的
        clock_policy / last_clock_time 两个字段同样成对出现、皆可整体省略：
        缺省时按 allow_regression 且无水位恢复（旧快照兼容）；显式给出时
        clock_policy 必须合法，last_clock_time 必须是 None 或有限的绝对
        时刻，且 allow_regression 不接受非 None 水位。任一字段非法或只给出
        其一时整次恢复抛出 ValueError 且原状态（含当前时钟策略与水位）完全
        保持。成功后以
        副本整体替换状态并返回 None，恢复出的容器（含对齐列表与历史）与传入
        快照相互独立。恢复后一律由本实例当前时间源按既有的 expiry <= now 边界
        判定过期，不隐式清理、不释放队列槽位、不延长去重窗口；恢复不读取
        注入时钟，reject_regression 的水位以快照值为准并在此后的首次实时
        采样时继续生效。
        """
        (values, events, event_expiries, seen, max_queue, overflow_policy,
         discard_history, discard_history_limit,
         event_receipts, next_receipt,
         clock_policy, last_clock_time, event_metadata) = _parse_snapshot(snapshot)
        self.values = values
        self.events = events
        self.event_expiries = event_expiries
        self.event_receipts = event_receipts
        self.event_metadata = event_metadata
        self._next_receipt = next_receipt
        self.seen = seen
        self.max_queue = max_queue
        self.overflow_policy = overflow_policy
        self._discard_history = discard_history
        self.discard_history_limit = discard_history_limit
        self.clock_policy = clock_policy
        self._last_clock_time = last_clock_time
        return None
