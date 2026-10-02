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

    def cleanup(self):
        now = self.clock()
        values_removed = 0
        for key in [k for k, (_, expiry) in self.values.items() if expiry <= now]:
            del self.values[key]
            values_removed += 1
        dedupe_removed = 0
        for key in [k for k, expiry in self.seen.items() if expiry <= now]:
            del self.seen[key]
            dedupe_removed += 1
        # 已排入队列的事件不受影响
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

    def pop(self):
        return self.events.popleft() if self.events else None

    def queue_status(self):
        # 纯查询：不读取时钟、不触发清理、不改变队列
        return Result(size=len(self.events), max_queue=self.max_queue)
