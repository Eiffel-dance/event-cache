import unittest

import app
from app import EventCache, Result, ClockRegressionError


class ClockPolicyConstructionTest(unittest.TestCase):
    def test_default_policy_is_allow_regression(self):
        c = EventCache(lambda: 0)
        self.assertEqual(c.clock_policy, 'allow_regression')
        status = c.clock_status()
        self.assertEqual(status.policy, 'allow_regression')
        self.assertIsNone(status.last_time)

    def test_explicit_none_policy_is_default(self):
        c = EventCache(lambda: 0, clock_policy=None)
        self.assertEqual(c.clock_status().policy, 'allow_regression')

    def test_explicit_allow_regression(self):
        c = EventCache(lambda: 0, clock_policy='allow_regression')
        self.assertEqual(c.clock_status().policy, 'allow_regression')

    def test_invalid_policy_raises_without_sampling(self):
        calls = []

        def clock():
            calls.append(1)
            return 0

        for bad in ('strict', '', 1, True, False, 0.5, object()):
            with self.assertRaises(ValueError):
                EventCache(clock, clock_policy=bad)
        self.assertEqual(calls, [])

    def test_allow_regression_keeps_existing_behavior(self):
        now = [100]
        c = EventCache(lambda: now[0])  # 默认策略：允许回退
        c.put('a', 'v1', 10)
        now[0] = 50  # 时钟回退
        c.put('b', 'v2', 10)  # 不抛出
        self.assertEqual(c.get('a'), 'v1')  # 旧的过期记录按新时刻重新有效
        self.assertEqual(c.get('b'), 'v2')
        # allow 模式不维护水位
        self.assertIsNone(c.clock_status().last_time)


class RejectRegressionTest(unittest.TestCase):
    def setUp(self):
        self.now = [100]
        self.cache = EventCache(lambda: self.now[0],
                                clock_policy='reject_regression')

    def test_watermark_none_before_first_sample(self):
        self.assertIsNone(self.cache.clock_status().last_time)

    def test_watermark_advances_on_sample(self):
        self.cache.put('a', 'v', 10)
        self.assertEqual(self.cache.clock_status().last_time, 100)
        self.now[0] = 105
        self.cache.get('a')
        self.assertEqual(self.cache.clock_status().last_time, 105)

    def test_equal_time_is_allowed(self):
        self.cache.put('a', 'v', 10)
        self.cache.put('b', 'w', 10)  # 同一时刻：等于水位，继续
        self.assertEqual(self.cache.clock_status().last_time, 100)
        self.assertEqual(self.cache.get('a'), 'v')

    def test_regression_raises_and_preserves_state(self):
        self.cache.put('a', 'v', 10)
        self.cache.push('d', 'e', 10)
        before = (dict(self.cache.values), list(self.cache.events),
                  dict(self.cache.seen))
        self.now[0] = 99  # 回退
        with self.assertRaises(ClockRegressionError):
            self.cache.put('b', 'x', 10)
        with self.assertRaises(ClockRegressionError):
            self.cache.get('a')
        with self.assertRaises(ClockRegressionError):
            self.cache.get_with_reason('a')
        with self.assertRaises(ClockRegressionError):
            self.cache.push('d2', 'e2', 10)
        with self.assertRaises(ClockRegressionError):
            self.cache.push_with_receipt('d3', 'e3', 10)
        with self.assertRaises(ClockRegressionError):
            self.cache.push_expiring('d4', 'e4', 10, 5)
        with self.assertRaises(ClockRegressionError):
            self.cache.cleanup()
        with self.assertRaises(ClockRegressionError):
            self.cache.cleanup_expired_events()
        with self.assertRaises(ClockRegressionError):
            self.cache.discard_expired_events()
        with self.assertRaises(ClockRegressionError):
            self.cache.cleanup_all_expired()
        with self.assertRaises(ClockRegressionError):
            self.cache.push_batch([('d5', 'e5', 10)])
        with self.assertRaises(ClockRegressionError):
            self.cache.apply_batch([('put', 'k', 'v', 10)])
        with self.assertRaises(ClockRegressionError):
            self.cache.pop_live_batch()
        with self.assertRaises(ClockRegressionError):
            self.cache.peek_live_batch()
        # 状态与水位均未改变
        self.assertEqual(dict(self.cache.values), before[0])
        self.assertEqual(list(self.cache.events), before[1])
        self.assertEqual(dict(self.cache.seen), before[2])
        self.assertEqual(self.cache.clock_status().last_time, 100)
        # 时钟恢复后一切照常
        self.now[0] = 100
        self.assertEqual(self.cache.get('a'), 'v')

    def test_regression_error_carries_times(self):
        self.cache.put('a', 'v', 10)
        self.now[0] = 42
        with self.assertRaises(ClockRegressionError) as ctx:
            self.cache.cleanup()
        self.assertEqual(ctx.exception.observed, 42)
        self.assertEqual(ctx.exception.last_time, 100)

    def test_watermark_advances_on_unsuccessful_outcomes(self):
        # 即使结果为 expired / queue_full / 没有可清理项，成功采样也推进水位
        self.cache.put('a', 'v', 5)
        self.now[0] = 200
        self.assertIsNone(self.cache.get('a'))  # expired
        self.assertEqual(self.cache.clock_status().last_time, 200)
        limited = EventCache(lambda: self.now[0], max_queue=1,
                             clock_policy='reject_regression')
        limited.push('d1', 'e1', 100)
        self.now[0] = 300
        r = limited.push_with_reason('d2', 'e2', 100)  # queue_full
        self.assertFalse(r.accepted)
        self.assertEqual(limited.clock_status().last_time, 300)
        self.now[0] = 400
        self.assertEqual(self.cache.cleanup().values_removed, 0)  # 无可清理项
        self.assertEqual(self.cache.clock_status().last_time, 400)

    def test_clock_exception_propagates_without_state_change(self):
        state = {'fail': False}

        def clock():
            if state['fail']:
                raise RuntimeError('boom')
            return self.now[0]

        c = EventCache(clock, clock_policy='reject_regression')
        c.put('a', 'v', 10)
        state['fail'] = True
        with self.assertRaises(RuntimeError):
            c.put('b', 'x', 10)
        self.assertEqual(c.clock_status().last_time, 100)
        self.assertNotIn('b', c.values)
        state['fail'] = False
        self.assertEqual(c.get('a'), 'v')

    def test_non_clock_paths_unaffected_by_regression(self):
        self.cache.put('a', 'v', 10)
        r = self.cache.push_with_receipt('d', 'e', 10)
        self.now[0] = 50  # 回退
        # 不读取时钟的入口继续可用
        self.assertTrue(self.cache.delete('a'))
        self.assertEqual(self.cache.pop_with_receipt().receipt, r.receipt)
        self.assertEqual(self.cache.peek(), [])
        self.assertEqual(self.cache.queue_status().size, 0)
        self.assertEqual(self.cache.cancel(999).reason, 'missing')
        self.assertEqual(self.cache.discard_history(), [])
        self.assertIsNone(self.cache.get('missing'))  # 缺失键不读时钟
        self.assertEqual(self.cache.get_with_reason('missing').reason, 'missing')
        snap = self.cache.snapshot()  # 快照路径不读时钟
        self.assertEqual(snap['clock_policy'], 'reject_regression')
        status = self.cache.clock_status()
        self.assertEqual(status.last_time, 100)

    def test_pop_live_batch_empty_queue_reads_no_clock(self):
        self.now[0] = 50  # 尚未采样，无所谓；先采样再回退
        c = EventCache(lambda: self.now[0], clock_policy='reject_regression')
        c.put('a', 'v', 10)
        self.now[0] = 10
        # 空队列 / limit=0 不读时钟，不触发回退检查
        self.assertEqual(c.pop_live_batch().events, [])
        self.assertEqual(c.pop_live_batch(0).events, [])
        self.assertEqual(c.peek_live_batch().events, [])

    def test_resize_queue_reads_clock_only_when_shrinking(self):
        c = EventCache(lambda: self.now[0], max_queue=5,
                       clock_policy='reject_regression')
        c.push('d1', 'e1', 100)
        c.push('d2', 'e2', 100)
        self.now[0] = 90  # 回退
        # 扩容不读时钟：不受回退影响
        c.resize_queue(10)
        self.assertEqual(c.clock_status().last_time, 100)
        # 需要挤出的缩容要观测时间：回退时抛出且配置不变
        with self.assertRaises(ClockRegressionError):
            c.resize_queue(1)
        self.assertEqual(c.max_queue, 10)
        self.assertEqual(c.queue_status().size, 2)
        self.now[0] = 110
        r = c.resize_queue(1)
        self.assertEqual(len(r.discarded), 1)
        self.assertEqual(c.clock_status().last_time, 110)


class ClockSnapshotTest(unittest.TestCase):
    def test_default_policy_snapshot_shape_unchanged(self):
        c = EventCache(lambda: 0)
        c.put('a', 'v', 10)
        snap = c.snapshot()
        self.assertNotIn('clock_policy', snap)
        self.assertNotIn('last_time', snap)

    def test_reject_policy_snapshot_includes_clock_fields(self):
        now = [100]
        c = EventCache(lambda: now[0], clock_policy='reject_regression')
        snap = c.snapshot()
        self.assertEqual(snap['clock_policy'], 'reject_regression')
        self.assertIsNone(snap['last_time'])  # 尚未采样
        c.put('a', 'v', 10)
        snap = c.snapshot()
        self.assertEqual(snap['last_time'], 100)

    def test_restore_roundtrip_preserves_policy_and_watermark(self):
        now = [100]
        c = EventCache(lambda: now[0], clock_policy='reject_regression')
        c.put('a', 'v', 10)
        c.push('d', 'e', 10)
        snap = c.snapshot()
        now[0] = 200
        other = EventCache(lambda: now[0])
        other.restore(snap)
        status = other.clock_status()
        self.assertEqual(status.policy, 'reject_regression')
        self.assertEqual(status.last_time, 100)
        # 恢复后的水位继续生效：回退到 90 被拒绝
        now[0] = 90
        with self.assertRaises(ClockRegressionError):
            other.cleanup()
        now[0] = 105  # 'a' 到期点为 110：仍有效
        self.assertEqual(other.get('a'), 'v')

    def test_old_snapshot_restores_as_allow_regression(self):
        now = [100]
        c = EventCache(lambda: now[0], clock_policy='reject_regression')
        c.put('a', 'v', 10)
        c.restore({'values': {'a': ('v', 150)}, 'events': [], 'seen': {},
                   'max_queue': None})
        status = c.clock_status()
        self.assertEqual(status.policy, 'allow_regression')
        self.assertIsNone(status.last_time)
        now[0] = 90  # 回退不再被拒绝
        c.put('b', 'w', 10)

    def test_restore_invalid_clock_fields_preserves_state(self):
        c = EventCache(lambda: 0, clock_policy='reject_regression')
        c.put('a', 'v', 10)  # 水位 0
        base = {'values': {}, 'events': [], 'seen': {}, 'max_queue': None}
        for extra in ({'clock_policy': 'strict'},
                      {'clock_policy': 1},
                      {'clock_policy': 'reject_regression',
                       'last_time': 'soon'},
                      {'clock_policy': 'reject_regression',
                       'last_time': float('nan')},
                      {'clock_policy': 'reject_regression', 'last_time': True},
                      {'clock_policy': 'allow_regression', 'last_time': 5}):
            snap = dict(base)
            snap.update(extra)
            with self.assertRaises(ValueError):
                c.restore(snap)
        # 整次恢复保持原状态：策略、水位与数据不变
        self.assertEqual(c.clock_status().policy, 'reject_regression')
        self.assertEqual(c.clock_status().last_time, 0)
        self.assertEqual(c.get('a'), 'v')

    def test_restore_does_not_sample_clock(self):
        calls = []

        def clock():
            calls.append(1)
            return 0

        c = EventCache(clock, clock_policy='reject_regression')
        c.restore({'values': {}, 'events': [], 'seen': {}, 'max_queue': None,
                   'clock_policy': 'reject_regression', 'last_time': 50})
        self.assertEqual(calls, [])
        self.assertEqual(c.clock_status().last_time, 50)


class ReplayClockPolicyTest(unittest.TestCase):
    def test_replay_does_not_read_clock_or_touch_watermark(self):
        now = [100]
        c = EventCache(lambda: now[0], clock_policy='reject_regression')
        c.put('k', 'v', 10)  # 水位 100
        results = c.replay_batch([
            (50, ('put', 'a', 'x', 100)),  # 记录时刻低于水位：回放不比较
            (60, ('push', 'd', 'e', 10)),
            (70, ('cleanup',)),
            (80, ('get', 'a')),
        ])
        self.assertEqual(results[3], 'x')
        # 回放不读取注入时钟，也不改变实时水位
        self.assertEqual(c.clock_status().last_time, 100)
        # 实时路径仍按水位判定
        now[0] = 90
        with self.assertRaises(ClockRegressionError):
            c.cleanup()


if __name__ == '__main__':
    unittest.main()
