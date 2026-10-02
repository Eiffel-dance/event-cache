import math
from collections import deque
from collections.abc import Mapping


_SNAPSHOT_FIELDS = frozenset(('values', 'events', 'seen', 'max_queue'))


class Result(dict):
    """结果对象：同时支持属性访问 (r.accepted) 与键访问 (r['accepted'])。"""

    def __getattribute__(self, name):
        # 数据字段优先于 dict 同名内省方法（如字段名恰为 'values'）；
        # 双下划线名称仍走默认查找，避免干扰 isinstance、拷贝/序列化等机制
        if isinstance(name, str) and not name.startswith('__') and dict.__contains__(self, name):
            return dict.__getitem__(self, name)
        return object.__getattribute__(self, name)

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)


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


def _parse_batch(batch):
    """在读取时钟或改变任何状态前完整解析并校验批次。

    批次不可迭代、条目不能解包为三元组或 window 非法时统一抛出 ValueError；
    dedupe 不可哈希、无法作为去重索引时抛出 TypeError。
    返回物化后的 (dedupe, event, window) 列表，供后续在同一时钟时刻逐项判定。
    """
    try:
        iterator = iter(batch)
    except TypeError:
        raise ValueError('batch must be an iterable of (dedupe, event, window) triples')
    entries = []
    for item in iterator:
        try:
            dedupe, event, window = item
        except (TypeError, ValueError):
            raise ValueError('each batch entry must be a (dedupe, event, window) triple')
        _check_duration(window, 'window')
        hash(dedupe)  # 不可哈希时原样抛出 TypeError
        entries.append((dedupe, event, window))
    return entries


def _parse_apply_batch(batch):
    """在读取时钟或改变任何状态前完整解析并校验事务批次。

    批次必须可迭代，每项为带标签的元组：
    ('put', key, value, ttl)、('delete', key)、
    ('push', dedupe, event, window) 或 ('cleanup',)。
    批次不可迭代、条目不是元组、标签未知、元组长度不符或 ttl/window
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
        elif tag == 'cleanup':
            if len(item) != 1:
                raise ValueError("'cleanup' operation must be ('cleanup',)")
            operations.append(('cleanup',))
        else:
            raise ValueError('unknown operation tag: %r' % (tag,))
    return operations


class EventCache:
    def __init__(self, clock, max_queue=None):
        _check_max_queue(max_queue)
        self.clock = clock
        self.max_queue = max_queue
        self.values = {}
        self.events = deque()
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
        # 无论值是否已过期都移除，返回调用前是否存在该键
        return self.values.pop(key, None) is not None

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

    def _try_push_at(self, dedupe, event, window, now):
        # 在指定时钟时刻判定一次入队：window 由调用方先行校验
        expiry = self.seen.get(dedupe)
        if expiry is not None and expiry > now:
            return 'dedupe_window'
        # 去重已可用但队列已满：拒绝且不登记新的去重占用
        if self.max_queue is not None and len(self.events) >= self.max_queue:
            return 'queue_full'
        # 记录不存在或到期点小于等于当前时刻：允许重新入队
        self.seen[dedupe] = now + window
        self.events.append(event)
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

    def push_batch(self, batch):
        # 先完整校验批次结构、每项 window 及 dedupe 可哈希性：在此之前不读取时钟、不改变任何状态
        entries = _parse_batch(batch)
        results = []
        if entries:
            # 整批使用同一时钟时刻，时间源只读取一次
            now = self.clock()
            for dedupe, event, window in entries:
                # 前项已立即更新 seen 与队列占用，后项据此继续判定
                reason = self._try_push_at(dedupe, event, window, now)
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
                else:  # 'cleanup'
                    values_removed, dedupe_removed = self._cleanup_at(now)
                    results.append(Result(
                        values_removed=values_removed,
                        dedupe_removed=dedupe_removed,
                    ))
        return results

    def pop(self):
        return self.events.popleft() if self.events else None

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
        return [self.events.popleft() for _ in range(count)]

    def queue_status(self):
        # 纯查询：不读取时钟、不触发清理、不改变队列
        return Result(size=len(self.events), max_queue=self.max_queue)

    def snapshot(self):
        """捕获当前状态的可检查副本。

        纯操作：不读取时钟、不隐式清理，已到期的 values/seen 记录原样保留。
        返回仅含 values、events、seen、max_queue 四个字段的 Result，支持键访问
        与属性访问。外层字典与事件列表均为复制，与缓存内部容器分离，双方后续
        增删互不影响；值对象与二元组按现有接口语义保留同一引用。
        """
        # 逐项浅拷贝 (value, expires_at) 二元组本身不可变，复制元组即可保留值引用
        values = {key: tuple(item) for key, item in self.values.items()}
        # list(deque) 生成元素为同一引用的新列表
        events = list(self.events)
        seen = dict(self.seen)
        return Result(values=values, events=events, seen=seen, max_queue=self.max_queue)

    def restore(self, snapshot):
        """从 snapshot() 风格的映射恢复状态，成功返回 None。

        校验在任何状态改动或时钟读取之前全部完成：snapshot 必须是恰含
        values/events/seen/max_queue 四个字段的映射；values 与 seen 必须是
        键值对容器（每项解包为二元组，values 的值还须恰为 (value, expires_at)
        二元组）；events 必须是元素列表；max_queue 必须是 None 或非负整数。
        结构不符统一抛出 ValueError；键不可哈希时原样抛出 TypeError。
        恢复一次性复制并替换四个状态字段，已到期记录原样保留，到期判定仍由
        目标实例当前时间源按 expiry <= now 边界惰性执行。
        """
        if not isinstance(snapshot, Mapping):
            raise ValueError('snapshot must be a mapping with values, events, seen, max_queue')
        if frozenset(snapshot) != _SNAPSHOT_FIELDS:
            raise ValueError('snapshot must contain exactly values, events, seen and max_queue')
        raw_values = snapshot['values']
        raw_events = snapshot['events']
        raw_seen = snapshot['seen']
        max_queue = snapshot['max_queue']
        # 先完整校验并物化为新容器：在此之前既不触碰现有状态也不读取时钟
        try:
            value_items = list(raw_values.items())
        except AttributeError:
            raise ValueError("snapshot 'values' must be a mapping of key to (value, expires_at)")
        values = {}
        for entry in value_items:
            if not isinstance(entry, tuple) or len(entry) != 2:
                raise ValueError("each 'values' entry must be a (key, (value, expires_at)) pair")
            key, item = entry
            if not isinstance(item, tuple) or len(item) != 2:
                raise ValueError("each 'values' item must be a (value, expires_at) tuple")
            values[key] = tuple(item)  # 键不可哈希时原样抛出 TypeError
        if not isinstance(raw_events, list):
            raise ValueError("snapshot 'events' must be a list of events")
        events = deque(raw_events)
        try:
            seen_items = list(raw_seen.items())
        except AttributeError:
            raise ValueError("snapshot 'seen' must be a mapping of dedupe key to expiry")
        seen = {}
        for entry in seen_items:
            if not isinstance(entry, tuple) or len(entry) != 2:
                raise ValueError("each 'seen' entry must be a (dedupe_key, expiry) pair")
            dedupe, expiry = entry
            seen[dedupe] = expiry  # 键不可哈希时原样抛出 TypeError
        _check_max_queue(max_queue)
        # 校验全部通过后一次性替换；不调用时钟、不触发任何清理
        self.values = values
        self.events = events
        self.seen = seen
        self.max_queue = max_queue
        return None
