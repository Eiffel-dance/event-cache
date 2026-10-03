import math
from collections import deque
from collections.abc import Mapping

_SNAPSHOT_FIELDS = frozenset(('values', 'events', 'seen', 'max_queue'))
_SNAPSHOT_FIELDS_WITH_EXPIRIES = _SNAPSHOT_FIELDS | frozenset(('event_expiries',))


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


def _parse_apply_batch(batch):
    """在读取时钟或改变任何状态前完整解析并校验事务批次。

    批次必须可迭代，每项为带标签的元组：
    ('put', key, value, ttl)、('delete', key)、
    ('push', dedupe, event, window)、
    ('push_expiring', dedupe, event, window, event_ttl) 或 ('cleanup',)。
    批次不可迭代、条目不是元组、标签未知、元组长度不符或 ttl/window/
    event_ttl 非法时统一抛出 ValueError；key/dedupe 不可哈希、无法作为
    缓存索引时抛出 TypeError。物化后的操作列表供调用方在同一时钟时刻顺序执行。
    """
    try:
        iterator = iter(batch)
    except TypeError:
        raise ValueError('operations must be an iterable of operation tuples')
    operations = []
    for item in iterator:
        if not isinstance(item, tuple) or len(item) == 0:
            raise ValueError('each operation must be a tagged tuple')
        tag = item[0]
        if tag == 'put':
            if len(item) != 4:
                raise ValueError("'put' operation must be ('put', key, value, ttl)")
            _, key, value, ttl = item
            _check_duration(ttl, 'ttl')
            hash(key)  # 不可哈希时原样抛出 TypeError
            operations.append(('put', key, value, ttl))
        elif tag == 'delete':
            if len(item) != 2:
                raise ValueError("'delete' operation must be ('delete', key)")
            _, key = item
            hash(key)
            operations.append(('delete', key))
        elif tag == 'push':
            if len(item) != 4:
                raise ValueError("'push' operation must be ('push', dedupe, event, window)")
            _, dedupe, event, window = item
            _check_duration(window, 'window')
            hash(dedupe)
            operations.append(('push', dedupe, event, window))
        elif tag == 'push_expiring':
            if len(item) != 5:
                raise ValueError(
                    "'push_expiring' operation must be ('push_expiring', dedupe, event, window, event_ttl)")
            _, dedupe, event, window, event_ttl = item
            _check_duration(window, 'window')
            _check_duration(event_ttl, 'event_ttl')
            hash(dedupe)
            operations.append(('push_expiring', dedupe, event, window, event_ttl))
        elif tag == 'cleanup':
            if len(item) != 1:
                raise ValueError("'cleanup' operation must be ('cleanup',)")
            operations.append(('cleanup',))
        else:
            raise ValueError('unknown operation tag: %r' % (tag,))
    return operations


def _parse_snapshot(snapshot):
    """在读取时钟或改变任何状态前完整解析并校验快照。

    快照必须是含 values、events、seen、max_queue 四个字段的映射，或在此
    基础上增加与 events 对齐的 event_expiries 字段：values 为
    key -> (value, expires_at) 的映射，seen 为去重键 -> 绝对到期时间的
    映射，events 为事件列表（按 FIFO 顺序），max_queue 为 None 或非负
    整数，event_expiries 为与 events 等长的列表，每项为 None（无事件
    TTL）或有限的绝对到期时刻。三类到期时间（values 的 expires_at、seen
    的到期时刻、event_expiries 的非 None 项）都只能是非 bool 的有限
    int/float，允许负数与已过期时刻，不解释为相对时长。字段缺失或多余、
    非映射/列表容器、二元组结构不符、event_expiries 长度不一致或任一
    到期值为 NaN/无穷/字符串/复合对象、max_queue 非法时统一抛出
    ValueError；键不可哈希时原样抛出 TypeError。
    校验期间一次性物化为全新的 dict/list/deque，供调用方随后整体替换状态。
    """
    if not isinstance(snapshot, Mapping):
        raise ValueError('snapshot must be a mapping with values, events, seen, max_queue')
    fields = frozenset(snapshot.keys())
    if fields != _SNAPSHOT_FIELDS and fields != _SNAPSHOT_FIELDS_WITH_EXPIRIES:
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
    _check_max_queue(snapshot['max_queue'])

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
    return values, events, event_expiries, seen, snapshot['max_queue']


class EventCache:
    def __init__(self, clock, max_queue=None):
        _check_max_queue(max_queue)
        self.clock = clock
        self.max_queue = max_queue
        self.values = {}
        self.events = deque()
        # 与 events 逐元素对齐：None 表示无事件 TTL，否则为绝对到期时刻
        self.event_expiries = deque()
        self.seen = {}

    def _put_at(self, key, value, ttl, now):
        # 以写入时刻加 ttl 记录到期点，并替换同 key 旧值；ttl 由调用方先行校验
        self.values[key] = (value, now + ttl)

    def put(self, key, value, ttl):
        _check_duration(ttl, 'ttl')
        now = self.clock()
        self._put_at(key, value, ttl, now)

    def get(self, key):
        item = self.values.get(key)
        if item is None:
            return None
        value, expiry = item
        now = self.clock()
        # 到期点小于或等于当前时刻即视为过期
        if expiry <= now:
            self.values.pop(key, None)
            return None
        return value

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
        now = self.clock()
        values_removed, dedupe_removed = self._cleanup_at(now)
        return Result(values_removed=values_removed, dedupe_removed=dedupe_removed)

    def cleanup_expired_events(self):
        """单次读取时钟，移除所有已到期的带 TTL 事件。

        到期边界与 values/seen 一致：到期点 <= 当前时刻即移除。未到期事件
        与未设置事件 TTL 的旧事件一律保留且相对顺序不变；values、seen 与
        max_queue 不受影响。返回 Result(events_removed=移除数量)。
        """
        now = self.clock()
        kept_events = deque()
        kept_expiries = deque()
        events_removed = 0
        for event, expiry in zip(self.events, self.event_expiries):
            if expiry is not None and expiry <= now:
                events_removed += 1
            else:
                kept_events.append(event)
                kept_expiries.append(expiry)
        self.events = kept_events
        self.event_expiries = kept_expiries
        return Result(events_removed=events_removed)

    def _try_push_at(self, dedupe, event, window, now, event_ttl=None):
        # 在指定时钟时刻判定一次入队：window/event_ttl 由调用方先行校验
        expiry = self.seen.get(dedupe)
        if expiry is not None and expiry > now:
            return 'dedupe_window'
        # 去重已可用但队列已满：拒绝且不登记新的去重占用
        if self.max_queue is not None and len(self.events) >= self.max_queue:
            return 'queue_full'
        # 记录不存在或到期点小于等于当前时刻：允许重新入队
        self.seen[dedupe] = now + window
        self.events.append(event)
        # 事件到期时刻 = 接受时刻 + event_ttl；event_ttl 为零即接受时已到期
        self.event_expiries.append(None if event_ttl is None else now + event_ttl)
        return None

    def _try_push(self, dedupe, event, window):
        # 校验失败时不读取时钟，也不产生事件或去重记录
        _check_duration(window, 'window')
        now = self.clock()
        return self._try_push_at(dedupe, event, window, now)

    def push(self, dedupe, event, window):
        return self._try_push(dedupe, event, window) is None

    def push_with_reason(self, dedupe, event, window):
        reason = self._try_push(dedupe, event, window)
        return Result(accepted=reason is None, reason=reason)

    def push_expiring(self, dedupe, event, window, event_ttl):
        return self._try_push_expiring(dedupe, event, window, event_ttl) is None

    def push_expiring_with_reason(self, dedupe, event, window, event_ttl):
        reason = self._try_push_expiring(dedupe, event, window, event_ttl)
        return Result(accepted=reason is None, reason=reason)

    def _try_push_expiring(self, dedupe, event, window, event_ttl):
        # event_ttl 为必选时长：None 等非法值同样在校验阶段抛出 ValueError，
        # 校验失败时不读取时钟，也不产生事件或去重记录
        _check_duration(window, 'window')
        _check_duration(event_ttl, 'event_ttl')
        now = self.clock()
        return self._try_push_at(dedupe, event, window, now, event_ttl)

    def push_batch(self, batch):
        # 先完整校验批次结构、每项 window/event_ttl 及 dedupe 可哈希性：在此之前不读取时钟、不改变任何状态
        entries = _parse_batch(batch)
        results = []
        if entries:
            # 整批使用同一时钟时刻，时间源只读取一次
            now = self.clock()
            for dedupe, event, window, event_ttl in entries:
                # 前项已立即更新 seen 与队列占用，后项据此继续判定
                reason = self._try_push_at(dedupe, event, window, now, event_ttl)
                results.append(Result(accepted=reason is None, reason=reason))
        return results

    def apply_batch(self, operations):
        # 先完整校验批次结构、标签、时长及键：在此之前不读取时钟、不改变任何状态
        parsed = _parse_apply_batch(operations)
        results = []
        if parsed:
            # 整批使用同一时钟时刻，时间源只读取一次；时钟抛出的异常原样转出
            now = self.clock()
            for op in parsed:
                tag = op[0]
                if tag == 'put':
                    _, key, value, ttl = op
                    self._put_at(key, value, ttl, now)
                    results.append(Result(accepted=True, reason=None))
                elif tag == 'delete':
                    _, key = op
                    results.append(Result(deleted=self.delete(key)))
                elif tag == 'push':
                    _, dedupe, event, window = op
                    # 前序操作（含 cleanup）已立即更新状态，本项据此在同一时刻判定
                    reason = self._try_push_at(dedupe, event, window, now)
                    results.append(Result(accepted=reason is None, reason=reason))
                elif tag == 'push_expiring':
                    _, dedupe, event, window, event_ttl = op
                    reason = self._try_push_at(dedupe, event, window, now, event_ttl)
                    results.append(Result(accepted=reason is None, reason=reason))
                else:  # 'cleanup'
                    values_removed, dedupe_removed = self._cleanup_at(now)
                    results.append(Result(
                        values_removed=values_removed,
                        dedupe_removed=dedupe_removed,
                    ))
        return results

    def pop(self):
        if not self.events:
            return None
        self.event_expiries.popleft()
        return self.events.popleft()

    def pop_batch(self, limit=None):
        """按 FIFO 从队头批量取出事件。

        limit 为 None（缺省）时取出当前队列全部事件；为非负整数时最多取出
        该数量，数量不足只返回实际存在的事件。纯出队操作：不读取时钟、不触发
        过期清理、不改变 values/seen/max_queue，每个被取出的事件只释放一个
        队列容量位置。limit 为负数、浮点数、字符串、布尔值或其他非整数时
        抛出 ValueError，且不移除任何事件。
        """
        if limit is None:
            count = len(self.events)
        else:
            # bool 是 int 的子类，必须显式排除；浮点数（含 2.0）同样拒绝
            if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
                raise ValueError('limit must be None or a non-negative integer')
            count = min(limit, len(self.events))
        # 逐个 popleft 与连续调用 pop 的顺序和元素完全一致，事件为 None 也原样保留
        taken = []
        for _ in range(count):
            taken.append(self.events.popleft())
            self.event_expiries.popleft()
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
        if limit is None:
            count = len(self.events)
        else:
            # bool 是 int 的子类，必须显式排除；浮点数（含 2.0）同样拒绝
            if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
                raise ValueError('limit must be None or a non-negative integer')
            count = min(limit, len(self.events))
        # 只物化队头前缀副本，deque 本身保持不变
        return [self.events[i] for i in range(count)]

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
        保留到原去重窗口截止，下一次相同去重键继续遵循既有判定。

        limit 不是 None 且不是非 bool 的非负整数（负数、浮点数、字符串、
        布尔值等）时抛出 ValueError，抛出前不读取时钟、不改变任何状态；时钟
        抛出的异常原样传播。返回 Result(events=有效事件列表,
        discarded=丢弃结果列表)，两者均为普通 list。
        """
        if limit is not None:
            # bool 是 int 的子类，必须显式排除；浮点数（含 2.0）同样拒绝
            if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
                raise ValueError('limit must be None or a non-negative integer')
            if limit == 0:
                # 显式零配额：不读时钟、不扫描、不改状态
                return Result(events=[], discarded=[])
        if not self.events:
            # 空队列不读取时钟
            return Result(events=[], discarded=[])
        now = self.clock()
        events = []
        discarded = []
        # 从队头逐项判定：有效事件与过期事件都已出列，停止扫描后剩余元素
        # 自然保持原序留在 deque 中，槽位不被提前释放
        while self.events:
            event = self.events.popleft()
            expiry = self.event_expiries.popleft()
            if expiry is not None and expiry <= now:
                # 到期边界与 values/seen/cleanup_expired_events 一致：<= 即过期
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
        if limit is not None:
            # bool 是 int 的子类，必须显式排除；浮点数（含 2.0）同样拒绝
            if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
                raise ValueError('limit must be None or a non-negative integer')
            if limit == 0:
                # 显式零配额：不读时钟、不扫描、不改状态
                return Result(events=[], discarded=[])
        if not self.events:
            # 空队列不读取时钟
            return Result(events=[], discarded=[])
        now = self.clock()
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

    def queue_status(self):
        # 纯查询：不读取时钟、不触发清理、不改变队列
        return Result(size=len(self.events), max_queue=self.max_queue)

    def snapshot(self):
        """捕获某一时刻的可检查、可恢复状态快照。

        纯查询：不读取时钟、不触发任何惰性或显式清理，快照中的过期 values/seen
        记录与未出队事件一律原样保留。队列中没有带 TTL 事件时返回只含
        values、events、seen、max_queue 四个字段的 Result；存在带 TTL 事件时
        增加与 events 对齐的 event_expiries 字段，无 TTL 的旧事件以 None
        表示。外层字典与事件列表均为与缓存分离的副本，随后任一方增删都不会
        影响另一方；value 与事件对象按既有接口语义保留引用。
        """
        snap = Snapshot(
            values=dict(self.values),
            events=list(self.events),
            seen=dict(self.seen),
            max_queue=self.max_queue,
        )
        if any(expiry is not None for expiry in self.event_expiries):
            snap['event_expiries'] = list(self.event_expiries)
        return snap

    def restore(self, snapshot):
        """从快照一次性恢复 values、events、seen、max_queue（及事件到期信息）。

        先完整解析并校验快照：在此之前不读取时钟、不改变任何状态，校验失败时
        原状态、队列顺序和容量完全保持。接受不含 event_expiries 的旧格式
        （恢复后所有事件均无 TTL）与含 event_expiries 的新格式。成功后以副本
        整体替换状态并返回 None，恢复出的容器与传入快照相互独立。恢复后一律
        由本实例当前时间源按既有的 expiry <= now 边界判定过期，不隐式清理、
        不释放队列槽位、不延长去重窗口。
        """
        values, events, event_expiries, seen, max_queue = _parse_snapshot(snapshot)
        self.values = values
        self.events = events
        self.event_expiries = event_expiries
        self.seen = seen
        self.max_queue = max_queue
        return None
