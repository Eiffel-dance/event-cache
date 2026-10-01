import unittest

import app
from app import EventCache


class ManualClock:
    """可手动推进的时钟，并记录读取次数，便于验证单次读取语义。"""

    def __init__(self, start=100):
        self.now = start
        self.reads = 0

    def __call__(self):
        self.reads += 1
        return self.now

    def advance(self, delta):
        self.now += delta


class SmokeTest(unittest.TestCase):
    def test_import(self):
        self.assertTrue(app)


class PutGetTest(unittest.TestCase):
    def setUp(self):
        self.clock = ManualClock()
        self.cache = EventCache(self.clock)

    def test_unexpired_returns_raw_value(self):
        self.cache.put("k", {"v": 1}, 10)
        self.clock.advance(10 - 1)
        self.assertEqual(self.cache.get("k"), {"v": 1})

    def test_expired_get_deletes_and_returns_none(self):
        self.cache.put("k", "v", 10)
        self.clock.advance(10)
        self.assertIsNone(self.cache.get("k"))
        self.assertNotIn("k", self.cache.values)
        # 再次读取仍然是 None，且不需要时钟判定已删除的键。
        reads_before = self.clock.reads
        self.assertIsNone(self.cache.get("k"))
        self.assertEqual(self.clock.reads, reads_before)

    def test_zero_ttl_expires_at_same_instant(self):
        self.cache.put("k", "v", 0)
        # 同一时刻读取：到期点 <= 当前时刻，视为过期。
        self.assertIsNone(self.cache.get("k"))

    def test_put_replaces_existing_value(self):
        self.cache.put("k", "old", 10)
        self.clock.advance(5)
        self.cache.put("k", "new", 10)
        self.clock.advance(9)  # 距第二次写入 9，旧值若未替换此刻已过期
        self.assertEqual(self.cache.get("k"), "new")

    def test_get_missing_returns_none(self):
        self.assertIsNone(self.cache.get("missing"))

    def test_invalid_ttl_raises_and_preserves_cache(self):
        self.cache.put("k", "v", 10)
        snapshot = dict(self.cache.values)
        for bad in (-1, -0.5, float("inf"), float("-inf"), float("nan"),
                    "10", None, True, False, [10]):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    self.cache.put("k2", "x", bad)
                with self.assertRaises(ValueError):
                    self.cache.put("k", "changed", bad)
        self.assertEqual(self.cache.values, snapshot)
        self.assertEqual(self.cache.get("k"), "v")


class DeleteTest(unittest.TestCase):
    def setUp(self):
        self.clock = ManualClock()
        self.cache = EventCache(self.clock)

    def test_delete_existing_returns_true(self):
        self.cache.put("k", "v", 10)
        self.assertTrue(self.cache.delete("k"))
        self.assertIsNone(self.cache.get("k"))

    def test_delete_missing_returns_false(self):
        self.assertFalse(self.cache.delete("k"))
        self.cache.put("k", "v", 1)
        self.clock.advance(5)  # 已过期但键仍在
        self.assertTrue(self.cache.delete("k"))
        self.assertFalse(self.cache.delete("k"))

    def test_delete_does_not_touch_queue(self):
        self.cache.push("d1", "e1", 10)
        self.cache.put("k", "v", 1)
        self.cache.delete("k")
        self.assertEqual(self.cache.pop(), "e1")
        self.assertIsNone(self.cache.pop())


class PushPopTest(unittest.TestCase):
    def setUp(self):
        self.clock = ManualClock()
        self.cache = EventCache(self.clock)

    def test_success_true_duplicate_false_fifo(self):
        self.assertTrue(self.cache.push("a", "e1", 10))
        self.assertTrue(self.cache.push("b", "e2", 10))
        self.assertFalse(self.cache.push("a", "e1-dup", 10))
        self.assertEqual(self.cache.pop(), "e1")
        self.assertEqual(self.cache.pop(), "e2")
        self.assertIsNone(self.cache.pop())

    def test_window_boundary_allows_re_enqueue_at_expiry(self):
        self.assertTrue(self.cache.push("a", "e1", 10))  # 到期点 110
        self.clock.advance(9)  # 109：仍在窗口内
        self.assertFalse(self.cache.push("a", "e2", 10))
        self.clock.advance(1)  # 110：到期点 <= 当前时刻，可重新入队
        self.assertTrue(self.cache.push("a", "e2", 10))
        self.assertEqual([self.cache.pop(), self.cache.pop()], ["e1", "e2"])

    def test_zero_window_expires_at_same_instant(self):
        self.assertTrue(self.cache.push("a", "e1", 0))
        # window=0 的去重记录到期点恰为当前时刻；按 <= 边界，
        # 同一时刻的下一次相同 dedupe 键已可重新入队（与 ttl=0 对称）。
        self.assertTrue(self.cache.push("a", "e2", 0))
        self.clock.advance(1)
        self.assertTrue(self.cache.push("a", "e3", 0))
        self.assertEqual(
            [self.cache.pop() for _ in range(3)], ["e1", "e2", "e3"]
        )

    def test_invalid_window_raises_and_creates_nothing(self):
        for bad in (-1, float("inf"), float("nan"), "5", None, True):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    self.cache.push("d", "e", bad)
                with self.assertRaises(ValueError):
                    self.cache.push_with_reason("d", "e", bad)
        self.assertEqual(self.cache.seen, {})
        self.assertIsNone(self.cache.pop())

    def test_push_with_reason_shapes(self):
        ok = self.cache.push_with_reason("a", "e1", 10)
        self.assertTrue(ok["accepted"])
        self.assertIsNone(ok["reason"])
        self.assertTrue(ok.accepted)
        blocked = self.cache.push_with_reason("a", "e2", 10)
        self.assertFalse(blocked["accepted"])
        self.assertEqual(blocked["reason"], "dedupe_window")
        self.assertFalse(blocked.accepted)
        self.assertEqual(self.cache.pop(), "e1")
        self.assertIsNone(self.cache.pop())

    def test_push_and_push_with_reason_share_dedupe_state(self):
        self.assertTrue(self.cache.push("a", "e1", 10))
        blocked = self.cache.push_with_reason("a", "e2", 10)
        self.assertEqual((blocked.accepted, blocked.reason),
                         (False, "dedupe_window"))
        self.clock.advance(10)
        ok = self.cache.push_with_reason("a", "e2", 10)
        self.assertEqual((ok.accepted, ok.reason), (True, None))


class CleanupTest(unittest.TestCase):
    def setUp(self):
        self.clock = ManualClock()
        self.cache = EventCache(self.clock)

    def test_cleanup_counts_and_removes_only_expired(self):
        self.cache.put("live", 1, 100)
        self.cache.put("dead", 2, 5)
        self.cache.put("boundary", 3, 10)  # 到期点恰好等于推进后的时刻
        self.cache.push("d1", "ev1", 5)    # 去重到期点 105
        self.cache.push("d2", "ev2", 100)
        self.clock.advance(10)
        result = self.cache.cleanup()
        self.assertEqual(result["values_removed"], 2)
        self.assertEqual(result["dedupe_removed"], 1)
        self.assertEqual(result.values_removed, 2)
        self.assertEqual(result.dedupe_removed, 1)
        self.assertEqual(self.cache.get("live"), 1)
        self.assertIsNone(self.cache.get("dead"))
        self.assertIsNone(self.cache.get("boundary"))
        # 被清理的 dedupe 键可重新入队；仍有效的继续拦截。
        self.assertTrue(self.cache.push("d1", "ev3", 100))
        self.assertFalse(self.cache.push("d2", "ev4", 100))

    def test_cleanup_does_not_touch_queued_events(self):
        self.cache.push("d1", "ev1", 1)
        self.clock.advance(50)
        result = self.cache.cleanup()
        self.assertEqual(result.dedupe_removed, 1)
        self.assertEqual(result.values_removed, 0)
        # 去重记录虽过期，已入队事件原样保留且顺序不变。
        self.assertEqual(self.cache.pop(), "ev1")
        self.assertIsNone(self.cache.pop())

    def test_cleanup_empty_is_zero(self):
        result = self.cache.cleanup()
        self.assertEqual((result.values_removed, result.dedupe_removed), (0, 0))


class ClockReadTest(unittest.TestCase):
    """涉及时间判定的公开操作每次调用只读取一次注入时钟。"""

    def test_single_clock_read_per_call(self):
        clock = ManualClock()
        cache = EventCache(clock)

        clock.reads = 0
        cache.put("k", "v", 10)
        self.assertEqual(clock.reads, 1)

        clock.reads = 0
        cache.get("k")
        self.assertEqual(clock.reads, 1)

        clock.reads = 0
        cache.push("d", "e", 10)
        self.assertEqual(clock.reads, 1)

        clock.reads = 0
        cache.push_with_reason("d", "e2", 10)
        self.assertEqual(clock.reads, 1)

        clock.reads = 0
        cache.cleanup()
        self.assertEqual(clock.reads, 1)

        clock.reads = 0
        cache.delete("k")
        self.assertEqual(clock.reads, 0)
        cache.pop()
        self.assertEqual(clock.reads, 0)

    def test_deterministic_with_manually_advanced_clock(self):
        clock = ManualClock(0)
        cache = EventCache(clock)
        # 若某次调用内部读取多次时钟，自增时钟会让结果不确定。
        auto_clock = ManualClock(0)

        def ticking():
            v = auto_clock.now
            auto_clock.now += 1
            auto_clock.reads += 1
            return v

        ticking_cache = EventCache(ticking)
        ticking_cache.put("k", "v", 5)   # 到期点 0+5=5
        self.assertEqual(ticking_cache.get("k"), "v")  # 当前时刻 1
        ticking_cache.put("z", "v0", 0)
        self.assertIsNone(ticking_cache.get("z"))     # 到期点 2 <= 当前时刻 3
        # 普通手动时钟结果一致
        cache.put("k", "v", 5)
        self.assertEqual(cache.get("k"), "v")
        cache.put("z", "v0", 0)
        self.assertIsNone(cache.get("z"))


if __name__ == "__main__":
    unittest.main()
