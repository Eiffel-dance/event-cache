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


class EventCache:
    def __init__(self, clock, max_queue=None):
        # None 表示不限容量；否则必须是非负整数，布尔值不视为有效容量
        if max_queue is not None and (
            isinstance(max_queue, bool) or not isinstance(max_queue, int) or max_queue < 0
        ):
            raise ValueError('max_queue must be None or a non-negative integer')
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

    def _try_push(self, dedupe, event, window):
        # 校验失败时不读取时钟，也不产生事件或去重记录
        _check_duration(window, 'window')
        now = self.clock()
        expiry = self.seen.get(dedupe)
        # 去重窗口优先于容量：两者同时成立时固定报告 dedupe_window
        if expiry is not None and expiry > now:
            return 'dedupe_window'
        # 去重已可用但没有空槽位：拒绝且不改动队列与去重记录
        if self.max_queue is not None and len(self.events) >= self.max_queue:
            return 'queue_full'
        # 记录不存在或到期点小于等于当前时刻：允许重新入队
        self.seen[dedupe] = now + window
        self.events.append(event)
        return None

    def push(self, dedupe, event, window):
        return self._try_push(dedupe, event, window) is None

    def push_with_reason(self, dedupe, event, window):
        reason = self._try_push(dedupe, event, window)
        return Result(accepted=reason is None, reason=reason)

    def pop(self):
        return self.events.popleft() if self.events else None

    def queue_status(self):
        # 纯查询：不读取时钟、不触发清理、不改变队列顺序
        return Result(size=len(self.events), max_queue=self.max_queue)
