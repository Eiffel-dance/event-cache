import math
from collections import deque


class Result(dict):
    """结果对象：同时支持属性访问 (r.accepted) 与键访问 (r['accepted'])。"""

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

    批次不可迭代、条目不能解包为三元组或 window 非法时统一抛出 ValueError。
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
        # 先完整校验批次结构与每项 window：在此之前不读取时钟、不改变任何状态
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
