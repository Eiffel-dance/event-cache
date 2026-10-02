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


def _parse_operations(operations):
    """在读取时钟或改变任何状态前完整解析并校验 apply_batch 的操作序列。

    序列不可迭代、条目不是元组、标签未知、元组长度不符或 ttl/window 非法时
    统一抛出 ValueError；键（含去重键）不可哈希时原样抛出 TypeError。
    返回物化后的操作元组列表，供后续在同一时钟时刻按序执行。
    """
    try:
        iterator = iter(operations)
    except TypeError:
        raise ValueError('operations must be an iterable of operation tuples')
    entries = []
    for item in iterator:
        if not isinstance(item, tuple) or not item:
            raise ValueError('each operation must be a non-empty tagged tuple')
        tag = item[0]
        if tag == 'put':
            if len(item) != 4:
                raise ValueError('put operation must be ("put", key, value, ttl)')
            key, value, ttl = item[1], item[2], item[3]
            _check_duration(ttl, 'ttl')
            hash(key)  # 不可哈希的键无法用于缓存索引，抛出 TypeError
            entries.append(('put', key, value, ttl))
        elif tag == 'delete':
            if len(item) != 2:
                raise ValueError('delete operation must be ("delete", key)')
            key = item[1]
            hash(key)
            entries.append(('delete', key))
        elif tag == 'push':
            if len(item) != 4:
                raise ValueError('push operation must be ("push", dedupe, event, window)')
            dedupe, event, window = item[1], item[2], item[3]
            _check_duration(window, 'window')
            hash(dedupe)
            entries.append(('push', dedupe, event, window))
        elif tag == 'cleanup':
            if len(item) != 1:
                raise ValueError('cleanup operation must be ("cleanup",)')
            entries.append(('cleanup',))
        else:
            raise ValueError('unknown operation tag: %r' % (tag,))
    return entries


class EventCache:
    def __init__(self, clock, max_queue=None):
        _check_max_queue(max_queue)
        self.clock = clock
        self.max_queue = max_queue
        self.values = {}
        self.events = deque()
        self.seen = {}

    def put(self, key, value, ttl):
        _check_duration(ttl, 'ttl')
        now = self.clock()
        # 以写入时刻加 ttl 记录到期点，并替换同 key 旧值
        self.values[key] = (value, now + ttl)

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
        # 先完整遍历并校验所有操作：在此之前不读取时钟、不改变任何状态
        entries = _parse_operations(operations)
        results = []
        if not entries:
            # 空批次不读取时钟
            return results
        # 校验完成后只读取一次时钟；时钟抛出的异常原样转出，状态保持不变
        now = self.clock()
        for entry in entries:
            tag = entry[0]
            if tag == 'put':
                _, key, value, ttl = entry
                # 以批次时刻加 ttl 记录到期点，并替换同 key 旧值
                self.values[key] = (value, now + ttl)
                results.append(Result(accepted=True, reason=None))
            elif tag == 'delete':
                _, key = entry
                # 无论值是否已过期都移除，返回调用前是否存在该键
                deleted = self.values.pop(key, None) is not None
                results.append(Result(deleted=deleted))
            elif tag == 'push':
                _, dedupe, event, window = entry
                # 与 push_with_reason 相同的判定边界、优先级与 reason
                reason = self._try_push_at(dedupe, event, window, now)
                results.append(Result(accepted=reason is None, reason=reason))
            else:  # cleanup
                # 不触碰队列事件，也不释放队列槽位；后续操作立即看到清理后的状态
                values_removed, dedupe_removed = self._cleanup_at(now)
                results.append(Result(
                    values_removed=values_removed,
                    dedupe_removed=dedupe_removed,
                ))
        return results

    def pop(self):
        return self.events.popleft() if self.events else None

    def queue_status(self):
        # 纯查询：不读取时钟、不触发清理、不改变队列
        return Result(size=len(self.events), max_queue=self.max_queue)
