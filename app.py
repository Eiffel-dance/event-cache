import math
from collections import deque


class _Result(dict):
    """操作结果：同时支持属性访问与键访问，便于观察确定性语义。"""

    __slots__ = ()

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)


def _is_finite_duration(value):
    # bool 是 int 的子类，但此处不接受 True/False 作为时长。
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value >= 0
    )


class EventCache:
    def __init__(self, clock):
        self.clock = clock
        self.values = {}
        self.events = deque()
        self.seen = {}

    def put(self, key, value, ttl):
        if not _is_finite_duration(ttl):
            raise ValueError("ttl must be a finite number >= 0")
        now = self.clock()
        # 以写入时刻加 ttl 记录到期点，并替换同 key 旧值。
        self.values[key] = (value, now + ttl)

    def get(self, key):
        item = self.values.get(key)
        if item is None:
            return None
        value, expiry = item
        now = self.clock()
        if expiry <= now:
            self.values.pop(key, None)
            return None
        return value

    def delete(self, key):
        # 无论值是否已过期都移除；返回调用前是否存在该键。
        existed = key in self.values
        self.values.pop(key, None)
        return existed

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
        # 已排入队列的事件不在清理范围内。
        return _Result(values_removed=values_removed, dedupe_removed=dedupe_removed)

    def push(self, dedupe, event, window):
        return self._push(dedupe, event, window)[0]

    def push_with_reason(self, dedupe, event, window):
        accepted, reason = self._push(dedupe, event, window)
        return _Result(accepted=accepted, reason=reason)

    def _push(self, dedupe, event, window):
        if not _is_finite_duration(window):
            raise ValueError("window must be a finite number >= 0")
        now = self.clock()
        # 到期点 <= 当前时刻的去重记录失效，相同 dedupe 键可重新入队。
        self.seen = {k: expiry for k, expiry in self.seen.items() if expiry > now}
        if dedupe in self.seen:
            return False, "dedupe_window"
        self.seen[dedupe] = now + window
        self.events.append(event)
        return True, None

    def pop(self):
        return self.events.popleft() if self.events else None
