import math
from collections import deque
from collections.abc import Mapping

_SNAPSHOT_FIELDS = frozenset(('values', 'events', 'seen', 'max_queue'))
_SNAPSHOT_FIELDS_WITH_EXPIRIES = _SNAPSHOT_FIELDS | frozenset(('event_expiries',))

# 单项 push 入口“不设置事件有效期”的内部哨兵；None 本身不能作哨兵，
# 因为 push_expiring 必须把 event_ttl=None 判为非法并抛出 ValueError
_NO_EVENT_TTL = object()


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

    @property
    def event_expiries(self):
        # 新格式快照才有此字段，缺省视为全部事件不设事件有效期
        return self['event_expiries'] if 'event_expiries' in self else None


def _check_duration(value, name):
    """ttl/window 必须是有限且不小于零的数值，布尔值不视为有效时长。"""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError('%s must be a finite non-negative number' % name)
    if not math.isfinite(value) or value < 0:
        raise ValueError('%s must be a finite non-negative number' % name)


def _check_max_queue(value):
    """max_queue 必须是 None 或非负整数，布尔值不视为有效上限。"""
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError('max_queue must be None or a non-negative integer')


def _check_expiry_point(value):
    """快照中的绝对到期时刻必须是有限数值；布尔值不视为有效时刻。
    时刻的零点由注入时钟定义，故不要求非负。"""
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError('event expiry must be a finite number or None')


def _parse_batch(batch):
    """在读取时钟或改变任何状态前完整解析并校验批次。

    批次不可迭代、条目不能解包为三元组或四元组，或 window/event_ttl
    非法时统一抛出 ValueError；dedupe 不可哈希、无法作为去重索引时抛出
    TypeError。每条物化为 (dedupe, event, window, event_ttl)：三元组的
    event_ttl 为 None（旧事件，不设有效期），四元组在末尾显式给出。
    返回的列表供后续在同一时钟时刻逐项判定。
    """
    try:
        iterator = iter(batch)
    except TypeError:
        raise ValueError(
            'batch must be an iterable of (dedupe, event, window) '
            'or (dedupe, event, window, event_ttl) entries'
        )
    entries = []
    for item in iterator:
        try:
            parsed = tuple(item)
        except TypeError:
            raise ValueError(
                'each batch entry must be a (dedupe, event, window) '
                'or (dedupe, event, window, event_ttl) tuple'
            )
        if len(parsed) == 3:
            dedupe, event, window = parsed
            event_ttl = None
        elif len(parsed) == 4:
            dedupe, event, window, event_ttl = parsed
        else:
            raise ValueError(
                'each batch entry must be a (dedupe, event, window) '
                'or (dedupe, event, window, event_ttl) tuple'
            )
        # 按元组成员顺序校验时长，再预检 dedupe 可哈希性（与旧三元组一致）
        _check_duration(window, 'window')
        if event_ttl is not None:
            _check_duration(event_ttl, 'event_ttl')
        hash(dedupe)  # 不可哈希时原样抛出 TypeError
        entries.append((dedupe, event, window, event_ttl))
    return entries


def _parse_apply_batch(batch):
    """在读取时钟或改变任何状态前完整解析并校验事务批次。

    批次必须可迭代，每项为带标签的元组：
    ('put', key, value, ttl)、('delete', key)、
    ('push', dedupe, event, window)、
    ('push_expiring', dedupe, event, window, event_ttl) 或 ('cleanup',)。
    批次不可迭代、条目不是元组、标签未知、元组长度不符或 ttl/window/event_ttl
    非法时统一抛出 ValueError；key/dedupe 不可哈希、无法作为缓存索引时
    抛出 TypeError。物化后的操作列表供调用方在同一时钟时刻顺序执行。
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
                    "'push_expiring' operation must be "
                    "('push_expiring', dedupe, event, window, event_ttl)"
                )
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

    快照必须是恰好含 values、events、seen、max_queue 四个字段的映射；
    当且仅当存在带事件有效期的事件时，追加与 events 等长对齐的
    event_expiries 字段（旧事件对应元素为 None，其余为绝对到期时刻）。
    values 为 key -> (value, expires_at) 的映射，seen 为去重键 -> 绝对到期
    时间的映射，events 为事件列表（按 FIFO 顺序），max_queue 为 None 或
    非负整数。字段缺失或多余、非映射/列表容器、二元组结构不符、event_expiries
    不是列表或长度不匹配、到期信息非法或 max_queue 非法时统一抛出 ValueError；
    键不可哈希时原样抛出 TypeError。
    校验期间一次性物化为全新的 dict/list/deque，供调用方随后整体替换状态。
    """
    if not isinstance(snapshot, Mapping):
        raise ValueError('snapshot must be a mapping with values, events, seen, max_queue')
    fields = frozenset(snapshot.keys())
    if fields not in (_SNAPSHOT_FIELDS, _SNAPSHOT_FIELDS_WITH_EXPIRIES):
        raise ValueError(
            'snapshot must contain values, events, seen, max_queue '
            'and optionally event_expiries'
        )

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

    has_expiries = 'event_expiries' in snapshot
    raw_expiries = snapshot['event_expiries'] if has_expiries else None
    if has_expiries:
        if not isinstance(raw_expiries, list):
            raise ValueError('snapshot event_expiries must be a list')
        if len(raw_expiries) != len(raw_events):
            raise ValueError('event_expiries must align one-to-one with events')

    values = {}
    for key, item in raw_values.items():
        if not isinstance(item, tuple) or len(item) != 2:
            raise ValueError('each values entry must be a (value, expires_at) pair')
        hash(key)  # 不可哈希时原样抛出 TypeError
        values[key] = item
    seen = {}
    for key, expiry in raw_seen.items():
        hash(key)  # 不可哈希时原样抛出 TypeError
        seen[key] = expiry
    # list() 物化事件副本；值与事件对象按既有语义保留引用。
    # 事件有效期物化为与事件逐项对齐的 deque：旧事件为 None。
    events = deque(raw_events)
    if has_expiries:
        expiries = deque()
        saw_ttl = False
        for expiry in raw_expiries:
            if expiry is not None:
                _check_expiry_point(expiry)
                saw_ttl = True
            expiries.append(expiry)
        if not saw_ttl:
            # 字段存在但全部为 None：与旧格式等价，归一化为不带有效期状态
            expiries = None
    else:
        expiries = None
    return values, events, seen, snapshot['max_queue'], expiries


class EventCache:
    def __init__(self, clock, max_queue=None):
        _check_max_queue(max_queue)
        self.clock = clock
        self.max_queue = max_queue
        self.values = {}
        self.events = deque()
        # 与 events 逐项对齐：None 表示旧事件（不设事件有效期），数值为其
        # 绝对到期时刻。所有事件均为旧事件时保持为 None，以维持既有内部形状。
        self.event_expiries = None
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

    def _try_push_at(self, dedupe, event, window, now, event_ttl=None):
        # 在指定时钟时刻判定一次入队：window/event_ttl 由调用方先行校验。
        # event_ttl 为 None 时是不设有效期的旧事件（批次解析也以 None 表示）。
        expiry = self.seen.get(dedupe)
        if expiry is not None and expiry > now:
            return 'dedupe_window'
        # 去重已可用但队列已满：拒绝且不登记新的去重占用，也不记录事件有效期
        if self.max_queue is not None and len(self.events) >= self.max_queue:
            return 'queue_full'
        # 记录不存在或到期点小于等于当前时刻：允许重新入队
        self.seen[dedupe] = now + window
        self.events.append(event)
        if event_ttl is None:
            if self.event_expiries is not None:
                self.event_expiries.append(None)
        else:
            if self.event_expiries is None:
                # 首次出现带有效期事件：为全部旧事件补 None 占位
                self.event_expiries = deque([None] * (len(self.events) - 1))
            self.event_expiries.append(now + event_ttl)
        return None

    def _try_push(self, dedupe, event, window, event_ttl=_NO_EVENT_TTL):
        # 校验失败时不读取时钟，也不产生事件或去重记录。
        # 缺省（旧 push 入口）不设置事件有效期；显式传入 None 属于非法 TTL。
        _check_duration(window, 'window')
        if event_ttl is not _NO_EVENT_TTL:
            _check_duration(event_ttl, 'event_ttl')
        now = self.clock()
        ttl_at = None if event_ttl is _NO_EVENT_TTL else event_ttl
        return self._try_push_at(dedupe, event, window, now, ttl_at)

    def push(self, dedupe, event, window):
        return self._try_push(dedupe, event, window) is None

    def push_with_reason(self, dedupe, event, window):
        reason = self._try_push(dedupe, event, window)
        return Result(accepted=reason is None, reason=reason)

    def push_expiring(self, dedupe, event, window, event_ttl):
        return self._try_push(dedupe, event, window, event_ttl) is None

    def push_expiring_with_reason(self, dedupe, event, window, event_ttl):
        reason = self._try_push(dedupe, event, window, event_ttl)
        return Result(accepted=reason is None, reason=reason)

    def push_batch(self, batch):
        # 先完整校验批次结构、每项 window/event_ttl 及 dedupe 可哈希性：
        # 在此之前不读取时钟、不改变任何状态
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
        # 纯出队：不读取时钟，到期事件也按插入顺序原样返回；
        # 同步弹出对齐的有效期槽位，保持两条队列逐项对应
        if not self.events:
            return None
        if self.event_expiries is not None:
            self.event_expiries.popleft()
        return self.events.popleft()

    def pop_batch(self, limit=None):
        """按 FIFO 从队头批量取出事件。

        limit 为 None（缺省）时取出当前队列全部事件；为非负整数时最多取出
        该数量，数量不足只返回实际存在的事件。纯出队操作：不读取时钟、不触发
        过期清理、不改变 values/seen/max_queue，每个被取出的事件只释放一个
        队列容量位置并同步移除对齐的有效期槽位。limit 为负数、浮点数、字符串、
        布尔值或其他非整数时抛出 ValueError，且不移除任何事件。
        """
        if limit is None:
            count = len(self.events)
        else:
            # bool 是 int 的子类，必须显式排除；浮点数（含 2.0）同样拒绝
            if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
                raise ValueError('limit must be None or a non-negative integer')
            count = min(limit, len(self.events))
        # 逐个 popleft 与连续调用 pop 的顺序和元素完全一致，事件为 None 也原样保留
        if self.event_expiries is not None:
            for _ in range(count):
                self.event_expiries.popleft()
        return [self.events.popleft() for _ in range(count)]

    def cleanup_expired_events(self):
        """单次读取时钟，移除所有到期点小于等于当前时刻的带有效期事件。

        未到期事件（含排在已到期事件之后的）相对顺序保持不变；不触碰 values、
        seen，也不影响不设事件 TTL 的旧事件——旧事件只随对齐槽位整体重排，
        本身绝不被移除。返回 events_removed 数量。
        """
        if not self.event_expiries:
            # 没有任何带有效期事件：仍按契约读取一次时钟，但不改动队列
            self.clock()
            return Result(events_removed=0)
        now = self.clock()
        keep_events = deque()
        keep_expiries = deque()
        events_removed = 0
        keeps_ttl = False
        for event, expiry in zip(self.events, self.event_expiries):
            if expiry is not None and expiry <= now:
                events_removed += 1
                continue
            keep_events.append(event)
            keep_expiries.append(expiry)
            if expiry is not None:
                keeps_ttl = True
        self.events = keep_events
        # 剩余事件全部为旧事件时回归不带有效期的规范内部形状
        self.event_expiries = keep_expiries if keeps_ttl else None
        return Result(events_removed=events_removed)

    def queue_status(self):
        # 纯查询：不读取时钟、不触发清理、不改变队列
        return Result(size=len(self.events), max_queue=self.max_queue)

    def snapshot(self):
        """捕获某一时刻的可检查、可恢复状态快照。

        纯查询：不读取时钟、不触发任何惰性或显式清理，快照中的过期 values/seen
        记录与未出队事件一律原样保留。不存在带事件 TTL 的事件时，返回只含
        values、events、seen、max_queue 四个字段的 Result（既有格式不变）；
        一旦包含带有效期事件，追加与 events 等长对齐的 event_expiries 列表，
        旧事件对应元素为 None。外层字典与事件/到期列表均为与缓存分离的副本，
        随后任一方增删都不会影响另一方；value 与事件对象按既有接口语义保留引用。
        """
        if self.event_expiries is not None and any(
            expiry is not None for expiry in self.event_expiries
        ):
            return Snapshot(
                values=dict(self.values),
                events=list(self.events),
                seen=dict(self.seen),
                max_queue=self.max_queue,
                event_expiries=list(self.event_expiries),
            )
        return Snapshot(
            values=dict(self.values),
            events=list(self.events),
            seen=dict(self.seen),
            max_queue=self.max_queue,
        )

    def restore(self, snapshot):
        """从快照一次性恢复 values、events、seen、max_queue 及事件有效期。

        先完整解析并校验快照：在此之前不读取时钟、不改变任何状态，校验失败时
        原状态、队列顺序和容量完全保持。成功后以副本整体替换状态并返回 None，
        恢复出的容器与传入快照相互独立。无 event_expiries 字段（旧格式）时，
        所有事件一律不设有效期。恢复后一律由本实例当前时间源按既有的
        expiry <= now 边界判定过期，不隐式清理、不释放队列槽位、不延长去重窗口。
        """
        values, events, seen, max_queue, event_expiries = _parse_snapshot(snapshot)
        self.values = values
        self.events = events
        self.seen = seen
        self.max_queue = max_queue
        self.event_expiries = event_expiries
        return None
