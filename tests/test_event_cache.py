import unittest
import app
from app import EventCache, Result


class DeterministicTest(unittest.TestCase):
    def setUp(self):
        self.now = [100]
        self.cache = EventCache(lambda: self.now[0])

    def advance(self, seconds):
        self.now[0] += seconds

    # ---- put/get 基础语义 ----
    def test_put_get_within_ttl(self):
        self.cache.put('a', 'v', 10)
        self.assertEqual(self.cache.get('a'), 'v')
        self.advance(10)
        self.assertIsNone(self.cache.get('a'))  # 到期点 == 当前时刻视为过期

    def test_zero_ttl_expires_immediately(self):
        self.cache.put('a', 'v', 0)
        self.assertIsNone(self.cache.get('a'))

    def test_put_replaces_old_value(self):
        self.cache.put('a', 'old', 100)
        self.cache.put('a', 'new', 100)
        self.assertEqual(self.cache.get('a'), 'new')

    def test_expired_get_deletes_value(self):
        self.cache.put('a', 'v', 5)
        self.advance(6)
        self.assertIsNone(self.cache.get('a'))
        self.assertNotIn('a', self.cache.values)

    def test_falsy_stored_value(self):
        self.cache.put('a', 0, 10)
        self.assertEqual(self.cache.get('a'), 0)
        self.cache.put('b', None, 10)
        self.assertIsNone(self.cache.get('b'))
        self.assertIn('b', self.cache.values)  # None 是合法值，不是缺失

    # ---- ttl 校验 ----
    def test_invalid_ttl_raises_and_preserves_state(self):
        self.cache.put('a', 'v', 10)
        for bad in (-1, float('nan'), float('inf'), -float('inf'), '10', None, True):
            with self.assertRaises(ValueError):
                self.cache.put('a', 'new', bad)
        self.assertEqual(self.cache.get('a'), 'v')  # 旧值不被改变

    # ---- delete ----
    def test_delete_existing_returns_true(self):
        self.cache.put('a', 'v', 10)
        self.assertTrue(self.cache.delete('a'))
        self.assertIsNone(self.cache.get('a'))

    def test_delete_missing_returns_false(self):
        self.assertFalse(self.cache.delete('x'))

    def test_delete_expired_returns_true_and_removes(self):
        self.cache.put('a', 'v', 5)
        self.advance(6)
        self.assertTrue(self.cache.delete('a'))
        self.assertFalse(self.cache.delete('a'))

    def test_delete_falsy_values_report_existence_not_truthiness(self):
        # 结果只表达键是否存在：任意假值都与真值一样返回 True
        for key, value in (('n', None), ('f', False), ('z', 0), ('s', '')):
            self.cache.put(key, value, 10)
            self.assertTrue(self.cache.delete(key))
            self.assertIsNone(self.cache.get(key))
            self.assertNotIn(key, self.cache.values)
            self.assertFalse(self.cache.delete(key))  # 再次删除才返回 False

    def test_delete_none_value_then_get_is_none_and_invisible(self):
        self.cache.put('n', None, 10)
        self.assertTrue(self.cache.delete('n'))
        self.assertIsNone(self.cache.get('n'))
        self.assertNotIn('n', self.cache.values)  # 键不再可见，而非保留为 None
        self.assertFalse(self.cache.delete('n'))

    def test_delete_expired_falsy_value_returns_true(self):
        self.cache.put('n', None, 5)
        self.advance(6)
        self.assertTrue(self.cache.delete('n'))  # 到期记录仍存在 -> True
        self.assertFalse(self.cache.delete('n'))

    def test_delete_does_not_read_clock(self):
        calls = [0]
        cache = EventCache(lambda: calls.__setitem__(0, calls[0] + 1) or self.now[0])
        cache.put('a', None, 10)
        before = calls[0]
        self.assertTrue(cache.delete('a'))
        self.assertFalse(cache.delete('missing'))
        self.assertEqual(calls[0], before)  # 删除不读取注入时钟

    def test_delete_does_not_trigger_batch_cleanup(self):
        self.cache.put('a', 1, 0)
        self.cache.push('d', 'e', 0)
        self.advance(1)  # value 与 seen 均到期，事件仍在队列
        self.assertFalse(self.cache.delete('missing'))
        self.assertTrue(self.cache.delete('a'))
        # 去重记录不被顺带清理，队列顺序不变
        self.assertIn('d', self.cache.seen)
        self.assertEqual(self.cache.pop(), 'e')

    def test_delete_unhashable_key_raises_type_error_atomically(self):
        self.cache.put('a', 'v', 100)
        self.cache.push('d', 'e', 100)
        before = (dict(self.cache.values), list(self.cache.events), dict(self.cache.seen))
        with self.assertRaises(TypeError):
            self.cache.delete(['unhashable'])
        after = (dict(self.cache.values), list(self.cache.events), dict(self.cache.seen))
        self.assertEqual(before, after)  # TypeError 后缓存状态完全不变

    # ---- push/pop ----
    def test_push_returns_true_and_fifo(self):
        self.assertTrue(self.cache.push('d1', 'e1', 10))
        self.assertTrue(self.cache.push('d2', 'e2', 10))
        self.assertFalse(self.cache.push('d1', 'e1-dup', 10))
        self.assertEqual(self.cache.pop(), 'e1')
        self.assertEqual(self.cache.pop(), 'e2')
        self.assertIsNone(self.cache.pop())  # 空队列返回 None

    def test_dedupe_expires_at_boundary(self):
        self.cache.push('d', 'first', 10)
        self.advance(10)
        self.assertTrue(self.cache.push('d', 'second', 10))  # 到期点 <= 当前时刻可重入
        self.assertEqual(self.cache.pop(), 'first')
        self.assertEqual(self.cache.pop(), 'second')

    def test_push_with_reason_accepted(self):
        r = self.cache.push_with_reason('d', 'e', 10)
        self.assertTrue(r['accepted'])
        self.assertTrue(r.accepted)
        self.assertIsNone(r.reason)

    def test_push_with_reason_window_block(self):
        self.cache.push('d', 'e', 10)
        r = self.cache.push_with_reason('d', 'e2', 10)
        self.assertFalse(r.accepted)
        self.assertEqual(r.reason, 'dedupe_window')
        self.assertEqual(self.cache.pop(), 'e')  # 被拒事件未入队

    def test_push_and_push_with_reason_share_validation(self):
        for bad in (-1, float('nan'), float('inf'), True, 1.0j):
            with self.assertRaises(ValueError):
                self.cache.push('d', 'e', bad)
            with self.assertRaises(ValueError):
                self.cache.push_with_reason('d', 'e', bad)
        self.assertEqual(self.cache.pop(), None)
        self.assertEqual(self.cache.seen, {})

    def test_zero_window_uses_same_boundary(self):
        # window=0 => 到期点 == 当前时刻，按 <= 边界同一时刻即可重新入队
        self.assertTrue(self.cache.push('d', 'e1', 0))
        self.assertTrue(self.cache.push('d', 'e2', 0))
        self.assertEqual([self.cache.pop(), self.cache.pop()], ['e1', 'e2'])

    def test_positive_window_blocks_within_window(self):
        self.assertTrue(self.cache.push('d', 'e1', 10))
        self.advance(9)
        self.assertFalse(self.cache.push('d', 'e2', 10))
        self.advance(1)  # 到达到期点
        self.assertTrue(self.cache.push('d', 'e3', 10))

    # ---- cleanup ----
    def test_cleanup_removes_expired_only(self):
        self.cache.put('a', 1, 5)
        self.cache.put('b', 2, 50)
        self.cache.push('d1', 'e1', 5)
        self.cache.push('d2', 'e2', 50)
        self.advance(6)
        result = self.cache.cleanup()
        self.assertEqual(result.values_removed, 1)
        self.assertEqual(result.dedupe_removed, 1)
        self.assertNotIn('a', self.cache.values)
        self.assertIn('b', self.cache.values)
        self.assertNotIn('d1', self.cache.seen)
        self.assertIn('d2', self.cache.seen)

    def test_cleanup_does_not_touch_queue(self):
        self.cache.push('d1', 'queued-event', 0)
        self.advance(1)
        self.cache.cleanup()  # 去重记录到期，但队列事件保留
        self.assertEqual(self.cache.pop(), 'queued-event')

    def test_cleanup_idempotent(self):
        self.cache.put('a', 1, 0)
        self.assertEqual(self.cache.cleanup().values_removed, 1)
        self.assertEqual(self.cache.cleanup().values_removed, 0)

    # ---- 状态隔离 ----
    def test_delete_and_cleanup_preserve_order(self):
        self.cache.push('d1', 'e1', 100)
        self.cache.push('d2', 'e2', 100)
        self.cache.put('a', 1, 0)
        self.cache.delete('missing')
        self.cache.cleanup()
        self.assertEqual([self.cache.pop(), self.cache.pop()], ['e1', 'e2'])


class GetWithReasonTest(unittest.TestCase):
    def setUp(self):
        self.now = [100]
        self.clock_calls = [0]

        def clock():
            self.clock_calls[0] += 1
            return self.now[0]

        self.clock = clock
        self.cache = EventCache(clock)

    def advance(self, seconds):
        self.now[0] += seconds

    def assertResult(self, result, found, value, reason):
        self.assertIsInstance(result, Result)
        self.assertEqual(set(result), {'found', 'value', 'reason'})
        self.assertIs(result.found, found)
        self.assertIs(result['found'], found)
        self.assertIs(result.value, value)
        self.assertEqual(result['reason'], reason)

    # ---- 三种结果 ----
    def test_missing_key_returns_missing_without_clock(self):
        r = self.cache.get_with_reason('x')
        self.assertResult(r, False, None, 'missing')
        self.assertEqual(self.clock_calls[0], 0)

    def test_live_value_returns_found_with_reason_none(self):
        self.cache.put('a', 'v', 10)
        r = self.cache.get_with_reason('a')
        self.assertResult(r, True, 'v', None)
        self.assertIn('a', self.cache.values)  # 存活读取不移除
        self.assertEqual(self.clock_calls[0], 2)  # put 与读取各一次

    def test_falsy_live_values_still_found(self):
        for key, value in (('n', None), ('f', False), ('z', 0), ('s', '')):
            self.cache.put(key, value, 10)
        for key, value in (('n', None), ('f', False), ('z', 0), ('s', '')):
            r = self.cache.get_with_reason(key)
            self.assertIs(r.found, True)
            self.assertIs(r.value, value)  # 假值原样返回，而非当作缺失
            self.assertIsNone(r.reason)
            self.assertIn(key, self.cache.values)

    def test_expired_at_boundary_returns_expired_and_deletes(self):
        self.cache.put('a', 'v', 10)  # 到期点 110
        self.advance(10)
        r = self.cache.get_with_reason('a')
        self.assertResult(r, False, None, 'expired')
        self.assertNotIn('a', self.cache.values)  # 与 get 一样删除该键

    def test_expired_then_missing_on_next_read(self):
        self.cache.put('a', 'v', 0)
        self.assertEqual(self.cache.get_with_reason('a').reason, 'expired')
        # 已删除的键后续按 missing 返回，且不再读取时钟
        calls_before = self.clock_calls[0]
        r = self.cache.get_with_reason('a')
        self.assertResult(r, False, None, 'missing')
        self.assertEqual(self.clock_calls[0], calls_before)

    def test_reads_clock_exactly_once_when_key_exists(self):
        self.cache.put('a', 'v', 10)
        self.clock_calls[0] = 0
        self.cache.get_with_reason('a')
        self.assertEqual(self.clock_calls[0], 1)
        self.advance(100)
        self.cache.get_with_reason('a')  # 过期路径同样只读一次
        self.assertEqual(self.clock_calls[0], 2)

    # ---- 与 get 的共存与行为隔离 ----
    def test_get_behavior_unchanged(self):
        self.cache.put('a', 'v', 10)
        self.cache.put('n', None, 10)
        self.assertEqual(self.cache.get('a'), 'v')
        self.assertIsNone(self.cache.get('n'))
        self.assertIn('n', self.cache.values)
        self.advance(10)
        self.assertIsNone(self.cache.get('a'))
        self.assertNotIn('a', self.cache.values)

    def test_get_and_get_with_reason_share_expiry_side_effect(self):
        self.cache.put('a', 'v', 0)
        # get 先移除过期键后，诊断入口按 missing 报告
        self.assertIsNone(self.cache.get('a'))
        self.assertEqual(self.cache.get_with_reason('a').reason, 'missing')
        self.cache.put('b', 'v', 0)
        self.assertEqual(self.cache.get_with_reason('b').reason, 'expired')
        self.assertIsNone(self.cache.get('b'))  # 已被诊断入口移除

    # ---- 纯诊断：不触碰 seen/events/容量，不触发批量清理 ----
    def test_does_not_touch_seen_events_or_capacity(self):
        cache = EventCache(self.clock, max_queue=2)
        cache.put('k', 'v', 0)                  # 即刻过期的 value
        cache.put('live', 'L', 100)
        cache.push('d', 'e', 0)                 # 即刻过期的 seen，事件仍在队列
        cache.push_expiring('d2', 'ex', 100, 0)  # 即刻过期的带 TTL 事件
        self.advance(1)
        r = cache.get_with_reason('k')
        self.assertEqual(r.reason, 'expired')
        self.assertNotIn('k', cache.values)
        self.assertIn('live', cache.values)       # 不批量清理其他 value
        self.assertEqual(set(cache.seen), {'d', 'd2'})  # seen 不动
        self.assertEqual(list(cache.events), ['e', 'ex'])
        self.assertEqual(list(cache.event_expiries), [None, 100])
        self.assertEqual(cache.queue_status().size, 2)
        # 诊断不释放槽位：队列仍满
        self.assertEqual(cache.push_with_reason('d3', 'new', 100).reason, 'queue_full')

    def test_missing_read_does_not_read_clock_or_cleanup(self):
        self.cache.put('k', 'v', 0)
        self.cache.push('d', 'e', 0)
        self.advance(1)  # value 与 seen 均已到期
        calls_before = self.clock_calls[0]
        self.assertEqual(self.cache.get_with_reason('absent').reason, 'missing')
        self.assertEqual(self.clock_calls[0], calls_before)
        self.assertIn('k', self.cache.values)  # 不顺带清理
        self.assertIn('d', self.cache.seen)

    # ---- 异常原子性 ----
    def test_unhashable_key_raises_type_error_without_clock_or_state_change(self):
        self.cache.put('a', 'v', 100)
        before = dict(self.cache.values)
        with self.assertRaises(TypeError):
            self.cache.get_with_reason(['unhashable'])
        self.assertEqual(self.clock_calls[0], 1)  # 只有 put 读过
        self.assertEqual(dict(self.cache.values), before)

    def test_clock_exception_propagates_without_state_change(self):
        def bad_clock():
            raise RuntimeError('clock broken')

        cache = EventCache(bad_clock)
        cache.restore({'values': {'k': ('v', 1)}, 'events': [], 'seen': {}, 'max_queue': None})
        with self.assertRaises(RuntimeError):
            cache.get_with_reason('k')
        self.assertEqual(cache.values, {'k': ('v', 1)})
        # 缺失路径不接触坏时钟
        self.assertEqual(cache.get_with_reason('absent').reason, 'missing')


class RenewTest(unittest.TestCase):
    def setUp(self):
        self.now = [100]
        self.clock_calls = [0]

        def clock():
            self.clock_calls[0] += 1
            return self.now[0]

        self.clock = clock
        self.cache = EventCache(clock)

    def advance(self, seconds):
        self.now[0] += seconds

    def assertRenew(self, result, renewed, value, reason, expires_at):
        self.assertIsInstance(result, Result)
        self.assertEqual(set(result), {'renewed', 'value', 'reason', 'expires_at'})
        self.assertIs(result.renewed, renewed)
        self.assertIs(result['renewed'], renewed)
        self.assertIs(result.value, value)
        self.assertEqual(result['reason'], reason)
        self.assertEqual(result['expires_at'], expires_at)

    # ---- 续期成功 ----
    def test_renew_live_value_extends_expiry_without_replacing_value(self):
        self.cache.put('a', 'v', 10)  # 到期点 110
        r = self.cache.renew('a', 20)
        self.assertRenew(r, True, 'v', None, 120)
        self.assertEqual(self.cache.values['a'], ('v', 120))  # 值不变、到期点更新

    def test_renew_returns_absolute_expiry_for_deterministic_replay(self):
        self.cache.put('a', 'v', 10)
        self.advance(5)
        r = self.cache.renew('a', 7)
        self.assertEqual(r.expires_at, 112)  # 观察时刻 105 + 7
        self.assertEqual(self.cache.values['a'][1], 112)

    def test_renew_preserves_falsy_values(self):
        for key, value in (('n', None), ('f', False), ('z', 0), ('s', '')):
            self.cache.put(key, value, 10)
        for key, value in (('n', None), ('f', False), ('z', 0), ('s', '')):
            r = self.cache.renew(key, 5)
            self.assertIs(r.renewed, True)
            self.assertIs(r.value, value)  # None/False/0/'' 原样保留
            self.assertEqual(r.expires_at, 105)
            self.assertIs(self.cache.values[key][0], value)

    def test_renew_can_shorten_as_well_as_extend(self):
        self.cache.put('a', 'v', 100)
        r = self.cache.renew('a', 1)  # ttl 缩短同样合法
        self.assertRenew(r, True, 'v', None, 101)

    def test_zero_ttl_renew_succeeds_but_expires_immediately(self):
        self.cache.put('a', 'v', 100)
        r = self.cache.renew('a', 0)  # 观察时刻为 100：新到期点恰为 100
        self.assertRenew(r, True, 'v', None, 100)
        # 同一时刻边界：到期点 <= 观察时刻即过期
        self.assertEqual(self.cache.renew('a', 1).reason, 'expired')

    def test_repeated_renew_chains_expiry(self):
        self.cache.put('a', 'v', 10)   # 到期点 110
        self.cache.renew('a', 20)      # 观察时刻 100 -> 120
        self.advance(15)               # 115，仍存活
        r = self.cache.renew('a', 10)  # 125
        self.assertRenew(r, True, 'v', None, 125)
        self.advance(10)               # 125：边界过期
        self.assertEqual(self.cache.renew('a', 1).reason, 'expired')

    # ---- missing ----
    def test_missing_key_returns_missing_without_clock(self):
        r = self.cache.renew('x', 10)
        self.assertRenew(r, False, None, 'missing', None)
        self.assertEqual(self.clock_calls[0], 0)

    def test_missing_does_not_create_entry_or_seen(self):
        self.cache.renew('x', 10)
        self.assertEqual(dict(self.cache.values), {})
        self.assertEqual(dict(self.cache.seen), {})

    # ---- expired ----
    def test_expired_at_boundary_returns_expired_and_deletes(self):
        self.cache.put('a', 'v', 10)  # 到期点 110
        self.advance(10)
        r = self.cache.renew('a', 20)
        self.assertRenew(r, False, None, 'expired', None)
        self.assertNotIn('a', self.cache.values)  # 与 get 一样删除该键

    def test_expired_then_missing_on_next_renew(self):
        self.cache.put('a', 'v', 0)
        self.assertEqual(self.cache.renew('a', 10).reason, 'expired')
        calls_before = self.clock_calls[0]
        r = self.cache.renew('a', 10)
        self.assertRenew(r, False, None, 'missing', None)
        self.assertEqual(self.clock_calls[0], calls_before)  # missing 不读时钟

    # ---- 时钟约定 ----
    def test_reads_clock_exactly_once_when_key_exists(self):
        self.cache.put('a', 'v', 10)
        self.clock_calls[0] = 0
        self.cache.renew('a', 10)
        self.assertEqual(self.clock_calls[0], 1)
        self.advance(100)
        self.cache.renew('a', 10)  # 过期路径同样只读一次
        self.assertEqual(self.clock_calls[0], 2)

    # ---- 只影响 values ----
    def test_does_not_touch_seen_events_capacity_receipts_or_history(self):
        cache = EventCache(self.clock, max_queue=5, overflow_policy='drop_oldest')
        cache.put('k', 'L', 100)
        cache.push('d', 'e', 50)
        cache.push_expiring('d2', 'ex', 50, 50)
        before = (dict(cache.seen), list(cache.events), list(cache.event_expiries),
                  list(cache.event_receipts), cache._next_receipt,
                  [dict(h) for h in cache.discard_history()],
                  cache.max_queue, cache.overflow_policy)
        r = cache.renew('k', 90)
        self.assertRenew(r, True, 'L', None, 190)
        after = (dict(cache.seen), list(cache.events), list(cache.event_expiries),
                 list(cache.event_receipts), cache._next_receipt,
                 [dict(h) for h in cache.discard_history()],
                 cache.max_queue, cache.overflow_policy)
        self.assertEqual(before, after)

    def test_does_not_extend_or_clean_seen(self):
        self.cache.put('k', 'v', 100)
        self.cache.push('d', 'e', 2)  # seen['d'] 到期点 102
        self.advance(50)
        self.cache.renew('k', 100)
        self.assertEqual(self.cache.seen['d'], 102)  # seen 既不延长也不被清理

    def test_does_not_batch_clean_other_expired_values(self):
        self.cache.put('k', 'v', 100)
        self.cache.put('dead', 'x', 0)
        self.advance(1)
        self.cache.renew('k', 10)
        self.assertIn('dead', self.cache.values)  # 不顺带清理其他过期值

    # ---- 校验与原子性 ----
    def test_invalid_ttl_raises_value_error_without_clock_or_state_change(self):
        self.cache.put('a', 'v', 10)
        for bad in (-1, float('nan'), float('inf'), -float('inf'), '10', None,
                    True, False, [], {}):
            calls_before = self.clock_calls[0]
            with self.assertRaises(ValueError):
                self.cache.renew('a', bad)
            self.assertEqual(self.clock_calls[0], calls_before)
        self.assertEqual(self.cache.values['a'], ('v', 110))  # 旧到期点不变

    def test_unhashable_key_raises_type_error_without_clock_or_state_change(self):
        self.cache.put('a', 'v', 100)
        self.cache.push('d', 'e', 100)
        before = (dict(self.cache.values), list(self.cache.events), dict(self.cache.seen))
        with self.assertRaises(TypeError):
            self.cache.renew(['unhashable'], 10)
        self.assertEqual(self.clock_calls[0], 2)  # 只有 put 与 push 读过
        self.assertEqual(
            (dict(self.cache.values), list(self.cache.events), dict(self.cache.seen)),
            before)

    def test_clock_exception_propagates_without_state_change(self):
        def bad_clock():
            raise RuntimeError('clock broken')

        cache = EventCache(bad_clock)
        cache.restore({'values': {'k': ('v', 500)}, 'events': [], 'seen': {},
                       'max_queue': None})
        with self.assertRaises(RuntimeError):
            cache.renew('k', 10)
        self.assertEqual(cache.values, {'k': ('v', 500)})
        # 缺失路径不接触坏时钟
        self.assertEqual(cache.renew('absent', 10).reason, 'missing')

    def test_clock_regression_rejection_preserves_state(self):
        now_box = [10]
        cache = EventCache(lambda: now_box[0], clock_policy='reject_regression')
        cache.put('k', 'v', 100)  # 观察时刻 10，到期点 110，水位 10
        now_box[0] = 5
        with self.assertRaises(app.ClockRegressionError):
            cache.renew('k', 100)
        self.assertEqual(cache.values['k'], ('v', 110))  # 到期点不更新
        self.assertEqual(cache.clock_status().last_time, 10)  # 水位不回退


class CapacityTest(unittest.TestCase):
    def setUp(self):
        self.now = [100]
    def make(self, max_queue):
        return EventCache(lambda: self.now[0], max_queue=max_queue)

    # ---- 构造参数校验 ----
    def test_omitted_max_queue_means_unlimited(self):
        cache = EventCache(lambda: self.now[0])
        self.assertIsNone(cache.max_queue)
        for i in range(100):
            self.assertTrue(cache.push('d%d' % i, 'e%d' % i, 10))

    def test_none_max_queue_means_unlimited(self):
        cache = self.make(None)
        self.assertIsNone(cache.max_queue)
        for i in range(100):
            self.assertTrue(cache.push('d%d' % i, 'e%d' % i, 10))

    def test_invalid_max_queue_raises(self):
        for bad in (-1, -100, True, False, 1.5, 2.0, '3', [], object()):
            with self.assertRaises(ValueError):
                self.make(bad)

    def test_zero_max_queue_rejects_everything(self):
        cache = self.make(0)
        self.assertFalse(cache.push('d', 'e', 10))
        r = cache.push_with_reason('d', 'e', 10)
        self.assertFalse(r.accepted)
        self.assertEqual(r.reason, 'queue_full')
        self.assertIsNone(cache.pop())
        self.assertEqual(cache.seen, {})  # 拒绝不登记去重占用

    # ---- 容量上限 ----
    def test_queue_full_after_limit_reached(self):
        cache = self.make(2)
        self.assertTrue(cache.push('d1', 'e1', 10))
        self.assertTrue(cache.push('d2', 'e2', 10))
        self.assertFalse(cache.push('d3', 'e3', 10))
        r = cache.push_with_reason('d3', 'e3', 10)
        self.assertFalse(r.accepted)
        self.assertEqual(r.reason, 'queue_full')
        self.assertEqual([cache.pop(), cache.pop()], ['e1', 'e2'])
        self.assertIsNone(cache.pop())

    def test_rejection_does_not_create_or_extend_dedupe(self):
        cache = self.make(1)
        cache.push('d1', 'e1', 10)
        cache.push_with_reason('d2', 'e2', 5)  # 因 queue_full 被拒
        self.assertNotIn('d2', cache.seen)     # 未创建新的去重占用
        expiry_before = cache.seen['d1']
        cache.push_with_reason('d1', 'e1-dup', 50)  # dedupe_window 优先
        self.assertEqual(cache.seen['d1'], expiry_before)  # 旧占用未延长

    def test_dedupe_window_takes_priority_over_queue_full(self):
        cache = self.make(1)
        cache.push('d1', 'e1', 100)
        r = cache.push_with_reason('d1', 'e1-dup', 100)
        self.assertFalse(r.accepted)
        self.assertEqual(r.reason, 'dedupe_window')

    def test_pop_frees_slot_for_next_push(self):
        cache = self.make(1)
        cache.push('d1', 'e1', 0)
        self.assertFalse(cache.push('d2', 'e2', 0))
        self.assertEqual(cache.pop(), 'e1')
        self.assertTrue(cache.push('d2', 'e2', 0))  # 释放槽位后立即可入队
        self.assertEqual(cache.pop(), 'e2')

    def test_cleanup_does_not_free_slots(self):
        cache = self.make(1)
        cache.push('d1', 'e1', 5)
        self.now[0] += 10  # 去重记录到期
        result = cache.cleanup()
        self.assertEqual(result.dedupe_removed, 1)
        self.assertFalse(cache.push('d2', 'e2', 5))  # 事件未出队，槽位仍被占用
        self.assertEqual(cache.pop(), 'e1')

    # ---- queue_status ----
    def test_queue_status_unlimited(self):
        cache = self.make(None)
        s = cache.queue_status()
        self.assertEqual(s.size, 0)
        self.assertIsNone(s.max_queue)
        self.assertIsNone(s['max_queue'])
        cache.push('d', 'e', 10)
        self.assertEqual(cache.queue_status()['size'], 1)

    def test_queue_status_limited(self):
        cache = self.make(3)
        cache.push('d1', 'e1', 10)
        cache.push('d2', 'e2', 10)
        s = cache.queue_status()
        self.assertEqual(s.size, 2)
        self.assertEqual(s.max_queue, 3)
        cache.pop()
        self.assertEqual(cache.queue_status().size, 1)

    def test_queue_status_is_pure_query(self):
        calls = []
        cache = EventCache(lambda: calls.append(1) or self.now[0], max_queue=1)
        cache.push('d', 'e', 0)
        calls.clear()
        self.now[0] += 10  # 去重记录已到期，但查询不得触发清理
        s = cache.queue_status()
        self.assertEqual(calls, [])  # 未读取时钟
        self.assertEqual(s.size, 1)
        self.assertIn('d', cache.seen)  # 未触发清理
        self.assertEqual(cache.pop(), 'e')  # 队列顺序未变


class BatchTest(unittest.TestCase):
    def setUp(self):
        self.now = [100]
        self.clock_calls = [0]

        def clock():
            self.clock_calls[0] += 1
            return self.now[0]

        self.clock = clock
        self.cache = EventCache(clock)

    def advance(self, seconds):
        self.now[0] += seconds

    def make(self, max_queue):
        return EventCache(self.clock, max_queue=max_queue)

    # ---- 基本顺序与结果语义 ----
    def test_empty_batch_returns_empty_without_clock(self):
        self.assertEqual(self.cache.push_batch([]), [])
        self.assertEqual(self.clock_calls[0], 0)
        self.assertEqual(self.cache.queue_status().size, 0)

    def test_results_align_with_input_order_and_fifo(self):
        batch = [('d1', 'e1', 10), ('d2', 'e2', 10), ('d3', 'e3', 10)]
        results = self.cache.push_batch(batch)
        self.assertEqual(len(results), 3)
        for i, r in enumerate(results):
            self.assertIsInstance(r, Result)
            self.assertTrue(r['accepted'])
            self.assertTrue(r.accepted)
            self.assertIsNone(r.reason)
        self.assertEqual([self.cache.pop() for _ in range(3)], ['e1', 'e2', 'e3'])
        self.assertIsNone(self.cache.pop())

    def test_mixed_accepted_rejected_correspond_one_to_one(self):
        results = self.cache.push_batch([
            ('d1', 'e1', 10),
            ('d1', 'e1-dup', 10),   # 批内前项已登记 -> dedupe_window
            ('d2', 'e2', 10),
        ])
        self.assertEqual([(r.accepted, r.reason) for r in results], [
            (True, None),
            (False, 'dedupe_window'),
            (True, None),
        ])
        self.assertEqual([self.cache.pop(), self.cache.pop()], ['e1', 'e2'])

    def test_batch_reads_clock_exactly_once(self):
        results = self.cache.push_batch([('d%d' % i, 'e%d' % i, 10) for i in range(5)])
        self.assertTrue(all(r.accepted for r in results))
        self.assertEqual(self.clock_calls[0], 1)

    # ---- 同一时钟时刻下的窗口边界 ----
    def test_positive_window_blocks_later_item_in_same_batch(self):
        results = self.cache.push_batch([('d', 'e1', 10), ('d', 'e2', 10)])
        self.assertTrue(results[0].accepted)
        self.assertFalse(results[1].accepted)
        self.assertEqual(results[1].reason, 'dedupe_window')
        self.assertEqual(self.cache.pop(), 'e1')
        self.assertIsNone(self.cache.pop())

    def test_zero_window_allows_same_dedupe_within_batch_at_boundary(self):
        # window=0 => 到期点 == 当前时刻，按 <= 边界同批次即可重新入队
        results = self.cache.push_batch([('d', 'e1', 0), ('d', 'e2', 0), ('d', 'e3', 0)])
        self.assertTrue(all(r.accepted for r in results))
        self.assertEqual([self.cache.pop() for _ in range(3)], ['e1', 'e2', 'e3'])

    def test_prior_dedupe_expiring_at_batch_moment_is_allowed(self):
        self.cache.push('d', 'old', 10)  # 到期点 110
        self.advance(10)                 # 当前时刻恰为到期点
        results = self.cache.push_batch([('d', 'new', 0), ('d', 'again', 0)])
        self.assertTrue(all(r.accepted for r in results))

    def test_prior_unexpired_dedupe_blocks_first_batch_item(self):
        self.cache.push('d', 'old', 100)
        self.advance(50)
        results = self.cache.push_batch([('d', 'new', 0)])
        self.assertFalse(results[0].accepted)
        self.assertEqual(results[0].reason, 'dedupe_window')
        self.assertEqual(self.cache.pop(), 'old')
        self.assertIsNone(self.cache.pop())

    # ---- 容量与优先级 ----
    def test_queue_full_within_batch(self):
        cache = self.make(2)
        results = cache.push_batch([
            ('d1', 'e1', 10),
            ('d2', 'e2', 10),
            ('d3', 'e3', 10),  # 前两项已占满 -> queue_full
        ])
        self.assertEqual([(r.accepted, r.reason) for r in results], [
            (True, None), (True, None), (False, 'queue_full'),
        ])
        self.assertEqual(set(cache.seen), {'d1', 'd2'})

    def test_dedupe_window_priority_over_queue_full_in_batch(self):
        cache = self.make(1)
        cache.push('d1', 'e0', 100)  # 队列已满，d1 去重未到期
        results = cache.push_batch([
            ('d1', 'dup', 100),   # 先判去重 -> dedupe_window
            ('d2', 'e2', 100),    # 去重可用但满 -> queue_full
        ])
        self.assertEqual(results[0].reason, 'dedupe_window')
        self.assertEqual(results[1].reason, 'queue_full')

    def test_rejected_item_neither_queues_nor_registers_dedupe(self):
        cache = self.make(1)
        results = cache.push_batch([
            ('d1', 'e1', 10),
            ('d2', 'e2', 10),  # queue_full，不得登记 d2
        ])
        self.assertEqual(results[1].reason, 'queue_full')
        self.assertNotIn('d2', cache.seen)
        self.assertEqual(cache.pop(), 'e1')
        self.assertTrue(cache.push('d2', 'e2', 10))  # 未被预占，释放槽位后可入队

    def test_dedupe_rejection_does_not_extend_existing_seen(self):
        self.cache.push('d', 'e0', 10)
        expiry_before = self.cache.seen['d']
        self.cache.push_batch([('d', 'dup', 100)])
        self.assertEqual(self.cache.seen['d'], expiry_before)

    # ---- 接受任意可迭代对象 ----
    def test_accepts_generator_and_list_entries(self):
        gen = (entry for entry in [['d1', 'e1', 10], ['d2', 'e2', 10]])
        results = self.cache.push_batch(gen)
        self.assertTrue(all(r.accepted for r in results))
        self.assertEqual([self.cache.pop(), self.cache.pop()], ['e1', 'e2'])

    # ---- 校验原子性 ----
    def assert_state_untouched(self):
        self.assertEqual(self.cache.queue_status().size, 0)
        self.assertEqual(self.cache.seen, {})
        self.assertEqual(self.cache.values, {})

    def test_non_iterable_batch_raises_value_error(self):
        for bad in (None, 42, 3.14, True):
            with self.assertRaises(ValueError):
                self.cache.push_batch(bad)
        self.assertEqual(self.clock_calls[0], 0)
        self.assert_state_untouched()

    def test_entries_missing_members_raise_value_error(self):
        for bad in (
            [('d', 'e')],               # 缺少 window
            [('d', 'e', 10, 'x')],      # 多余成员
            ['not-a-triple'],           # 条目不可解包
            [123],                      # 条目不可迭代
            [('d1', 'e1', 10), ('d2', 'e2')],  # 后一条缺成员，前条也不得生效
        ):
            with self.assertRaises(ValueError):
                self.cache.push_batch(bad)
        self.assertEqual(self.clock_calls[0], 0)
        self.assert_state_untouched()

    def test_invalid_window_raises_value_error_atomically(self):
        for bad_window in (-1, float('nan'), float('inf'), -float('inf'), True, '10', None, 1.0j):
            with self.assertRaises(ValueError):
                self.cache.push_batch([
                    ('d1', 'e1', 10),                # 即使排在前面也不得生效
                    ('d2', 'e2', bad_window),
                ])
        self.assertEqual(self.clock_calls[0], 0)
        self.assert_state_untouched()

    def test_generator_failing_mid_validation_changes_nothing(self):
        def gen():
            yield ('d1', 'e1', 10)
            yield ('d2', 'e2', -1)  # window 非法

        with self.assertRaises(ValueError):
            self.cache.push_batch(gen())
        self.assertEqual(self.clock_calls[0], 0)
        self.assert_state_untouched()

    # ---- 不可哈希 dedupe 的预检 ----
    def test_unhashable_dedupe_raises_type_error_atomically(self):
        unhashable = ['d']  # list 不可哈希
        for batch in (
            [(unhashable, 'e1', 10), ('d2', 'e2', 10)],   # 首项不可哈希
            [('d1', 'e1', 10), (unhashable, 'e2', 10)],   # 末项不可哈希
            [('d1', 'e1', 10), (unhashable, 'e2', 10), ('d3', 'e3', 10)],  # 中间项
        ):
            with self.assertRaises(TypeError):
                self.cache.push_batch(batch)
        self.assertEqual(self.clock_calls[0], 0)  # 预检失败不读取时钟
        self.assert_state_untouched()

    def test_unhashable_dedupe_in_generator_changes_nothing(self):
        def gen():
            yield ('d1', 'e1', 10)
            yield ('d2', 'e2', 10)
            yield (['d3'], 'e3', 10)  # 后段出现不可哈希 dedupe

        with self.assertRaises(TypeError):
            self.cache.push_batch(gen())
        self.assertEqual(self.clock_calls[0], 0)
        self.assert_state_untouched()  # 前段有效条目也不得入队

    def test_unhashable_dedupe_preserves_existing_state(self):
        self.cache.put('k', 'v', 100)
        self.cache.push('d0', 'e0', 100)
        calls_before = self.clock_calls[0]
        with self.assertRaises(TypeError):
            self.cache.push_batch([('d1', 'e1', 10), ({'d': 2}, 'e2', 10)])
        self.assertEqual(self.clock_calls[0], calls_before)  # 未读取时钟
        self.assertEqual(self.cache.get('k'), 'v')
        self.assertEqual(self.cache.pop(), 'e0')
        self.assertIsNone(self.cache.pop())
        self.assertEqual(set(self.cache.seen), {'d0'})

    def test_batch_with_multiple_bad_entries_reports_first_in_order(self):
        # 先出现 window 错误：ValueError 优先于后项的 TypeError
        with self.assertRaises(ValueError):
            self.cache.push_batch([('d1', 'e1', -1), (['d2'], 'e2', 10)])
        # 先出现不可哈希 dedupe：TypeError 优先于后项的 ValueError
        with self.assertRaises(TypeError):
            self.cache.push_batch([(['d1'], 'e1', 10), ('d2', 'e2', -1)])
        self.assertEqual(self.clock_calls[0], 0)
        self.assert_state_untouched()

    def test_hashable_but_unusual_dedupe_keys_accepted(self):
        results = self.cache.push_batch([
            (None, 'e1', 10),
            (42, 'e2', 10),
            (('t', 1), 'e3', 10),
            (frozenset([1]), 'e4', 10),
        ])
        self.assertTrue(all(r.accepted for r in results))
        self.assertEqual([self.cache.pop() for _ in range(4)], ['e1', 'e2', 'e3', 'e4'])

    def test_invalid_batch_preserves_existing_state(self):
        self.cache.put('k', 'v', 100)
        self.cache.push('d0', 'e0', 100)
        calls_before = self.clock_calls[0]
        with self.assertRaises(ValueError):
            self.cache.push_batch([('d1', 'e1', 10), ('d2', 'e2', 'bad')])
        self.assertEqual(self.clock_calls[0], calls_before)  # 未读取时钟
        self.assertEqual(self.cache.get('k'), 'v')
        self.assertEqual(self.cache.pop(), 'e0')
        self.assertIsNone(self.cache.pop())
        self.assertEqual(set(self.cache.seen), {'d0'})

    # ---- 生命周期隔离 ----
    def test_batch_does_not_invoke_cleanup(self):
        # 已到期的 values/seen 记录在批量入队后保持原样（不隐式清理）
        self.cache.put('k', 'v', 5)
        self.cache.push('old', 'e-old', 5)
        self.advance(10)
        self.cache.push_batch([('d1', 'e1', 10)])
        self.assertIn('k', self.cache.values)
        self.assertIn('old', self.cache.seen)
        # 已排队事件的生命周期与顺序不变
        self.assertEqual([self.cache.pop(), self.cache.pop()], ['e-old', 'e1'])

    def test_single_entry_interfaces_unchanged_after_batch(self):
        self.clock_calls[0] = 0
        self.assertTrue(self.cache.push('d', 'e', 10))
        self.assertEqual(self.clock_calls[0], 1)
        self.assertFalse(self.cache.push('d', 'e2', 10))
        r = self.cache.push_with_reason('d', 'e3', 10)
        self.assertFalse(r.accepted)
        self.assertEqual(r.reason, 'dedupe_window')


class ApplyBatchTest(unittest.TestCase):
    def setUp(self):
        self.now = [100]
        self.clock_calls = [0]

        def clock():
            self.clock_calls[0] += 1
            return self.now[0]

        self.clock = clock
        self.cache = EventCache(clock)

    def advance(self, seconds):
        self.now[0] += seconds

    # ---- delete 结果与单项 delete 完全一致 ----
    def test_delete_none_value_reports_deleted_true(self):
        self.cache.put('n', None, 10)
        results = self.cache.apply_batch([('delete', 'n')])
        self.assertEqual(results[0].keys(), {'deleted'})
        self.assertIs(results[0].deleted, True)
        self.assertIsNone(self.cache.get('n'))
        self.assertNotIn('n', self.cache.values)

    def test_delete_falsy_values_match_single_delete(self):
        for key, value in (('n', None), ('f', False), ('z', 0), ('s', '')):
            single = EventCache(self.clock)
            single.put(key, value, 10)
            batched = EventCache(self.clock)
            batched.put(key, value, 10)
            self.assertIs(single.delete(key), batched.apply_batch([('delete', key)])[0].deleted)

    def test_repeat_delete_in_batch_reports_false_second_time(self):
        self.cache.put('n', None, 10)
        results = self.cache.apply_batch([('delete', 'n'), ('delete', 'n')])
        self.assertEqual([r.deleted for r in results], [True, False])

    def test_delete_expired_record_in_batch_reports_true(self):
        self.cache.put('n', None, 5)
        self.advance(6)
        results = self.cache.apply_batch([('delete', 'n'), ('delete', 'n')])
        self.assertEqual([r.deleted for r in results], [True, False])

    # ---- 混合操作按输入顺序处理、字段与时序语义不变 ----
    def test_mixed_operations_processed_in_order_single_clock_read(self):
        self.cache.put('k', 'old', 100)
        self.cache.push('d0', 'e0', 100)
        ops = [
            ('put', 'a', None, 10),
            ('delete', 'k'),
            ('delete', 'missing'),
            ('push', 'd1', 'e1', 10),
            ('cleanup',),
            ('delete', 'a'),
        ]
        before = self.clock_calls[0]
        results = self.cache.apply_batch(ops)
        self.assertEqual(self.clock_calls[0] - before, 1)  # 整批只读一次时钟
        self.assertEqual([set(r) for r in results], [
            {'accepted', 'reason'},
            {'deleted'},
            {'deleted'},
            {'accepted', 'reason'},
            {'values_removed', 'dedupe_removed'},
            {'deleted'},
        ])
        self.assertTrue(results[0].accepted)
        self.assertEqual([r.deleted for r in (results[1], results[2], results[5])],
                         [True, False, True])
        self.assertTrue(results[3].accepted)
        self.assertIsNone(results[3].reason)
        self.assertEqual((results[4].values_removed, results[4].dedupe_removed), (0, 0))
        self.assertNotIn('k', self.cache.values)
        self.assertNotIn('a', self.cache.values)
        self.assertEqual(self.cache.pop_batch(), ['e0', 'e1'])  # FIFO 不受影响

    def test_cleanup_within_batch_affects_later_operations(self):
        # 同刻先写即到期记录，cleanup 清掉后再删除同键应报 False
        results = self.cache.apply_batch([
            ('put', 'a', 'x', 0),
            ('cleanup',),
            ('delete', 'a'),
        ])
        self.assertEqual(results[1].values_removed, 1)
        self.assertIs(results[2].deleted, False)

    def test_empty_batch_reads_no_clock_and_changes_nothing(self):
        self.assertEqual(self.cache.apply_batch([]), [])
        self.assertEqual(self.clock_calls[0], 0)

    # ---- 失败原子性：校验在读时钟之前 ----
    def test_invalid_batch_raises_before_clock_and_preserves_state(self):
        self.cache.put('k', 'v', 100)
        self.cache.push('d0', 'e0', 100)
        before_state = (dict(self.cache.values), list(self.cache.events), dict(self.cache.seen))
        for bad, exc in (
            (None, ValueError),
            ([[42]], ValueError),
            ([('unknown',)], ValueError),
            ([('put', 'k', 'v')], ValueError),
            ([('push', 'd', 'e', -1)], ValueError),
            ([('put', 'k', 'v', -1)], ValueError),
            ([('delete', ['unhashable'])], TypeError),
        ):
            calls_before = self.clock_calls[0]
            with self.assertRaises(exc):
                self.cache.apply_batch(bad)
            self.assertEqual(self.clock_calls[0], calls_before)
        self.assertEqual(
            (dict(self.cache.values), list(self.cache.events), dict(self.cache.seen)),
            before_state,
        )

    def test_later_invalid_op_rolls_back_whole_batch(self):
        for bad, exc in (
            ([('put', 'a', 1, 10), ('push', 'd', 'e', -1)], ValueError),
            ([('put', 'a', 1, 10), ('delete', ['x'])], TypeError),
        ):
            cache = EventCache(self.clock)
            with self.assertRaises(exc):
                cache.apply_batch(bad)
            self.assertEqual(cache.values, {})
            self.assertEqual(cache.pop_batch(), [])

    # ---- renew 标签 ----
    def test_renew_op_matches_single_renew_fields_and_shared_clock(self):
        self.cache.put('a', 'v', 10)
        self.cache.put('b', None, 5)
        self.cache.put('c', 'v', 0)  # 同刻已到期
        before = self.clock_calls[0]
        results = self.cache.apply_batch([
            ('renew', 'a', 20),
            ('renew', 'missing', 1),
            ('renew', 'b', 0),
            ('renew', 'c', 10),
        ])
        self.assertEqual(self.clock_calls[0] - before, 1)  # 整批只读一次时钟
        self.assertEqual([set(r) for r in results],
                         [{'renewed', 'value', 'reason', 'expires_at'}] * 4)
        self.assertTrue(results[0].renewed)
        self.assertEqual(results[0].value, 'v')
        self.assertIsNone(results[0].reason)
        self.assertEqual(results[0].expires_at, 120)
        self.assertFalse(results[1].renewed)
        self.assertIsNone(results[1].value)
        self.assertEqual(results[1].reason, 'missing')
        self.assertIsNone(results[1].expires_at)
        self.assertTrue(results[2].renewed)
        self.assertIsNone(results[2].value)  # None 值原样保留
        self.assertEqual(results[2].expires_at, 100)
        self.assertEqual(results[3].reason, 'expired')  # 到期点 100 <= 观察时刻 100
        self.assertNotIn('c', self.cache.values)
        self.assertEqual(self.cache.values['a'], ('v', 120))
        self.assertEqual(self.cache.values['b'], (None, 100))

    def test_renew_within_batch_sees_earlier_ops_and_affects_later(self):
        results = self.cache.apply_batch([
            ('put', 'x', 1, 10),     # 到期点 110
            ('renew', 'x', 100),    # 110 -> 200
            ('renew', 'x', 1),      # 200 -> 101（续期也可缩短）
        ])
        self.assertEqual(results[1].expires_at, 200)
        self.assertEqual(results[2].expires_at, 101)
        self.assertEqual(self.cache.values['x'], (1, 101))

    def test_renew_in_batch_does_not_touch_queue_or_seen(self):
        self.cache.put('k', 'v', 10)
        self.cache.push('d', 'e', 50)
        before = (dict(self.cache.seen), list(self.cache.events),
                  list(self.cache.event_receipts), self.cache._next_receipt)
        self.cache.apply_batch([('renew', 'k', 90), ('renew', 'absent', 1)])
        after = (dict(self.cache.seen), list(self.cache.events),
                 list(self.cache.event_receipts), self.cache._next_receipt)
        self.assertEqual(before, after)
        self.assertEqual(self.cache.values['k'], ('v', 190))

    def test_invalid_renew_op_rejects_whole_batch_before_clock(self):
        self.cache.put('k', 'v', 100)
        for bad, exc in (
            ([('renew', 'x')], ValueError),                 # 长度不符
            ([('renew', 'x', 1, 2)], ValueError),
            ([('renew', 'x', -1)], ValueError),
            ([('renew', 'x', True)], ValueError),
            ([('renew', 'x', float('inf'))], ValueError),
            ([('put', 'a', 1, 10), ('renew', 'x', -1)], ValueError),
            ([('renew', ['unhashable'], 1)], TypeError),
            ([('renew', 'x', 1), ('renew', ['u'], 1)], TypeError),
        ):
            calls_before = self.clock_calls[0]
            state_before = dict(self.cache.values)
            with self.assertRaises(exc):
                self.cache.apply_batch(bad)
            self.assertEqual(self.clock_calls[0], calls_before)  # 不读时钟
            self.assertEqual(dict(self.cache.values), state_before)  # 无部分写入


class PopBatchTest(unittest.TestCase):
    def setUp(self):
        self.now = [100]
        self.clock_calls = [0]

        def clock():
            self.clock_calls[0] += 1
            return self.now[0]

        self.clock = clock
        self.cache = EventCache(clock)

    def advance(self, seconds):
        self.now[0] += seconds

    def make(self, max_queue):
        return EventCache(self.clock, max_queue=max_queue)

    def seed(self, events, window=100):
        for i, event in enumerate(events):
            self.assertTrue(self.cache.push('d%d' % i, event, window))
        self.clock_calls[0] = 0

    # ---- 基本取出语义 ----
    def test_default_limit_returns_all_in_fifo_order(self):
        self.seed(['e1', 'e2', 'e3'])
        self.assertEqual(self.cache.pop_batch(), ['e1', 'e2', 'e3'])
        self.assertEqual(self.cache.queue_status().size, 0)
        self.assertEqual(self.cache.pop_batch(), [])  # 空队列返回空列表

    def test_zero_limit_returns_empty_and_removes_nothing(self):
        self.seed(['e1', 'e2'])
        self.assertEqual(self.cache.pop_batch(0), [])
        self.assertEqual(self.cache.queue_status().size, 2)
        self.assertEqual(self.cache.pop(), 'e1')

    def test_limit_smaller_than_size_takes_prefix_only(self):
        self.seed(['e1', 'e2', 'e3', 'e4'])
        self.assertEqual(self.cache.pop_batch(2), ['e1', 'e2'])
        self.assertEqual(self.cache.queue_status().size, 2)
        self.assertEqual(self.cache.pop_batch(), ['e3', 'e4'])

    def test_limit_larger_than_size_returns_all_without_error(self):
        self.seed(['e1', 'e2'])
        self.assertEqual(self.cache.pop_batch(10), ['e1', 'e2'])
        self.assertEqual(self.cache.queue_status().size, 0)

    def test_explicit_none_limit_returns_all(self):
        self.seed(['e1', 'e2'])
        self.assertEqual(self.cache.pop_batch(None), ['e1', 'e2'])

    def test_none_events_are_preserved_as_elements(self):
        self.seed(['e1', None, 'e3'])
        self.assertEqual(self.cache.pop_batch(3), ['e1', None, 'e3'])

    def test_returns_list_instance(self):
        self.seed(['e1'])
        self.assertIsInstance(self.cache.pop_batch(), list)

    # ---- 与逐次 pop 完全等价 ----
    def test_matches_single_pop_on_separate_cache(self):
        events = ['a', None, 'b', None, 'c']
        self.seed(events)
        other = EventCache(lambda: self.now[0])
        for i, event in enumerate(events):
            other.push('k%d' % i, event, 100)
        # 批量分段取出与逐次 pop 的顺序和元素逐一相等
        self.assertEqual(self.cache.pop_batch(2), [other.pop(), other.pop()])
        self.assertEqual(self.cache.pop_batch(), [other.pop() for _ in range(3)])
        self.assertEqual(other.pop(), None)
        self.assertEqual(self.cache.pop_batch(), [])

    # ---- 时间与去重语义不受影响 ----
    def test_does_not_read_clock(self):
        self.seed(['e1', 'e2'])
        self.cache.pop_batch()
        self.assertEqual(self.clock_calls[0], 0)
        self.cache.pop_batch(1)
        self.assertEqual(self.clock_calls[0], 0)

    def test_does_not_trigger_cleanup(self):
        self.cache.put('k', 'v', 5)
        self.cache.push('d0', 'e-old', 5)
        self.advance(10)  # values 与 seen 均已到期，事件仍在队列中
        result = self.cache.pop_batch()
        # 到期事件不被跳过、不重排，按原序取出；到期记录原样保留
        self.assertEqual(result, ['e-old'])
        self.assertIn('k', self.cache.values)
        self.assertIn('d0', self.cache.seen)

    def test_rejected_events_never_appear_in_batch(self):
        self.assertTrue(self.cache.push('d', 'e1', 100))
        self.assertFalse(self.cache.push('d', 'rejected', 100))
        self.assertEqual(self.cache.pop_batch(), ['e1'])

    def test_expired_queued_events_keep_order_in_batch(self):
        self.cache.push('d1', 'e1', 0)
        self.advance(5)
        self.cache.push('d2', 'e2', 100)  # d1 去重已到期，互不影响
        self.advance(200)                 # 全部去重记录到期，队列不动
        self.assertEqual(self.cache.pop_batch(), ['e1', 'e2'])

    # ---- 容量释放 ----
    def test_popping_batch_frees_slots_for_push(self):
        cache = self.make(2)
        cache.push('d1', 'e1', 100)
        cache.push('d2', 'e2', 100)
        self.assertFalse(cache.push('d3', 'e3', 100))
        self.assertEqual(cache.pop_batch(1), ['e1'])  # 只释放一个槽位
        self.assertEqual(cache.queue_status().size, 1)
        self.assertTrue(cache.push('d3', 'e3', 100))
        self.assertFalse(cache.push('d4', 'e4', 100))  # 仍只剩一个槽位
        self.assertEqual(cache.pop_batch(), ['e2', 'e3'])
        self.assertTrue(cache.push('d4', 'e4', 100))

    def test_dedupe_window_still_applies_after_pop(self):
        cache = self.make(2)
        cache.push('d1', 'e1', 100)
        cache.pop_batch()
        # 槽位已释放，但去重窗口未到期：仍被去重拒绝
        r = cache.push_with_reason('d1', 'e1-again', 100)
        self.assertFalse(r.accepted)
        self.assertEqual(r.reason, 'dedupe_window')
        self.now[0] += 100  # 到达到期点
        self.assertTrue(cache.push('d1', 'e1-new', 100))

    # ---- limit 校验 ----
    def test_invalid_limit_raises_and_removes_nothing(self):
        self.seed(['e1', 'e2', 'e3'])
        for bad in (-1, -100, 1.5, 2.0, '3', [3], object(), True, False):
            with self.assertRaises(ValueError):
                self.cache.pop_batch(bad)
        self.assertEqual(self.clock_calls[0], 0)  # 校验失败不读取时钟
        self.assertEqual(self.cache.queue_status().size, 3)
        self.assertEqual(self.cache.pop_batch(), ['e1', 'e2', 'e3'])

    def test_plain_int_limit_accepted(self):
        self.seed(['e1', 'e2'])
        self.assertEqual(self.cache.pop_batch(1), ['e1'])
        self.assertEqual(self.cache.pop_batch(2), ['e2'])


class PopLiveBatchTest(unittest.TestCase):
    def setUp(self):
        self.now = [100]
        self.clock_calls = [0]

        def clock():
            self.clock_calls[0] += 1
            return self.now[0]

        self.clock = clock
        self.cache = EventCache(clock)

    def advance(self, seconds):
        self.now[0] += seconds

    def make(self, max_queue):
        return EventCache(self.clock, max_queue=max_queue)

    # ---- 基本结果形状 ----
    def test_returns_result_with_two_lists(self):
        self.cache.push('d1', 'e1', 100)
        r = self.cache.pop_live_batch()
        self.assertIsInstance(r, Result)
        self.assertIsInstance(r.events, list)
        self.assertIsInstance(r.discarded, list)
        self.assertEqual(r['events'], ['e1'])
        self.assertEqual(r.discarded, [])

    def test_empty_queue_reads_no_clock_and_returns_empty_lists(self):
        r = self.cache.pop_live_batch()
        self.assertEqual((r.events, r.discarded), ([], []))
        self.assertEqual(self.clock_calls[0], 0)
        r = self.cache.pop_live_batch(None)
        self.assertEqual((r.events, r.discarded), ([], []))
        self.assertEqual(self.clock_calls[0], 0)

    def test_zero_limit_reads_no_clock_even_on_nonempty_queue(self):
        self.cache.push_expiring('d1', 'e1', 100, 0)
        self.advance(50)
        self.clock_calls[0] = 0
        r = self.cache.pop_live_batch(0)
        self.assertEqual((r.events, r.discarded), ([], []))
        self.assertEqual(self.clock_calls[0], 0)
        self.assertEqual(self.cache.queue_status().size, 1)  # 状态不变

    def test_reads_clock_exactly_once(self):
        self.cache.push_expiring('d1', 'e1', 100, 5)
        self.cache.push('d2', 'e2', 100)
        self.cache.push_expiring('d3', 'e3', 100, 500)
        self.clock_calls[0] = 0
        self.cache.pop_live_batch()
        self.assertEqual(self.clock_calls[0], 1)
        self.cache.pop_live_batch(2)
        self.assertEqual(self.clock_calls[0], 1)  # 空队列不再读时钟

    # ---- 无 TTL 事件始终可出队 ----
    def test_plain_events_always_live_even_when_clock_far_ahead(self):
        self.cache.push('d1', 'e1', 0)
        self.cache.push('d2', 'e2', 0)
        self.advance(10000)
        r = self.cache.pop_live_batch()
        self.assertEqual(r.events, ['e1', 'e2'])
        self.assertEqual(r.discarded, [])
        self.assertEqual(self.cache.queue_status().size, 0)

    def test_none_event_is_a_live_element(self):
        self.cache.push('d1', None, 100)
        r = self.cache.pop_live_batch()
        self.assertEqual(r.events, [None])
        self.assertEqual(r.discarded, [])

    # ---- 过期识别、边界与顺序 ----
    def test_expired_events_are_discarded_in_original_order(self):
        self.cache.push_expiring('d1', 'e1', 100, 5)    # 到期点 105
        self.cache.push('d2', 'e2', 100)                # 无 TTL
        self.cache.push_expiring('d3', 'e3', 100, 500)  # 到期点 600
        self.advance(10)  # 当前 110
        r = self.cache.pop_live_batch()
        self.assertEqual(r.events, ['e2', 'e3'])  # 相对顺序保留
        self.assertEqual(len(r.discarded), 1)
        d = r.discarded[0]
        self.assertIsInstance(d, Result)
        self.assertEqual(d.event, 'e1')
        self.assertEqual(d['event'], 'e1')
        self.assertEqual(d.reason, 'event_ttl')

    def test_expired_items_at_head_middle_tail_keep_live_order(self):
        # 队头、队中、队尾均有过期项，有效期事件相对顺序不变
        self.cache.push_expiring('d1', 'x1', 100, 1)  # 过期
        self.cache.push_expiring('d2', 'a', 100, 500)  # 有效
        self.cache.push_expiring('d3', 'x2', 100, 1)  # 过期
        self.cache.push('d4', 'b', 100)               # 有效
        self.cache.push_expiring('d5', 'x3', 100, 1)  # 过期
        self.advance(10)
        r = self.cache.pop_live_batch()
        self.assertEqual(r.events, ['a', 'b'])
        self.assertEqual([d.event for d in r.discarded], ['x1', 'x2', 'x3'])
        self.assertTrue(all(d.reason == 'event_ttl' for d in r.discarded))
        self.assertEqual(self.cache.queue_status().size, 0)

    def test_boundary_expiry_equal_to_now_is_discarded(self):
        self.cache.push_expiring('dlive', 'live-soon', 100, 5)  # 到期点 105
        self.cache.push_expiring('deq', 'eq', 100, 5)          # 到期点 105
        self.advance(4)  # 当前 104
        r = self.cache.pop_live_batch(1)
        self.assertEqual(r.events, ['live-soon'])  # 104 < 105 仍有效，取 1 条即停
        self.advance(1)  # 当前恰为 105：到期点 <= 当前时刻
        r = self.cache.pop_live_batch()
        self.assertEqual(r.events, [])
        self.assertEqual([d.event for d in r.discarded], ['eq'])

    def test_zero_event_ttl_discarded_immediately(self):
        self.cache.push_expiring('d', 'e', 100, 0)  # 接受时即到期
        r = self.cache.pop_live_batch()
        self.assertEqual(r.events, [])
        self.assertEqual([d.event for d in r.discarded], ['e'])

    def test_all_expired_leaves_empty_queue(self):
        for i in range(3):
            self.cache.push_expiring('d%d' % i, 'e%d' % i, 100, 1)
        self.advance(10)
        r = self.cache.pop_live_batch()
        self.assertEqual(r.events, [])
        self.assertEqual([d.event for d in r.discarded], ['e0', 'e1', 'e2'])
        self.assertEqual(self.cache.pop_live_batch().discarded, [])

    # ---- limit 正整数：达到有效数量即停止扫描 ----
    def test_positive_limit_stops_after_enough_live_events(self):
        self.cache.push_expiring('d1', 'x1', 100, 1)  # 过期，先被扫描并丢弃
        self.cache.push_expiring('d2', 'a', 100, 500)  # 有效 #1
        self.cache.push_expiring('d3', 'x2', 100, 1)  # 过期，但不再扫描
        self.cache.push_expiring('d4', 'b', 100, 500)  # 不扫描
        self.advance(10)
        r = self.cache.pop_live_batch(1)
        self.assertEqual(r.events, ['a'])
        self.assertEqual([d.event for d in r.discarded], ['x1'])
        # 停止扫描后 x2、b 原样留在队列且顺序不变
        self.assertEqual(list(self.cache.events), ['x2', 'b'])

    def test_limit_counts_only_live_events(self):
        self.cache.push_expiring('d1', 'x1', 100, 1)
        self.cache.push_expiring('d2', 'x2', 100, 1)
        self.cache.push('d3', 'a', 100)
        self.cache.push('d4', 'b', 100)
        self.advance(10)
        r = self.cache.pop_live_batch(2)
        self.assertEqual(r.events, ['a', 'b'])
        self.assertEqual([d.event for d in r.discarded], ['x1', 'x2'])
        self.assertEqual(self.cache.queue_status().size, 0)

    def test_limit_larger_than_live_count_consumes_all(self):
        self.cache.push_expiring('d1', 'x', 100, 1)
        self.cache.push('d2', 'a', 100)
        self.advance(10)
        r = self.cache.pop_live_batch(10)
        self.assertEqual(r.events, ['a'])
        self.assertEqual([d.event for d in r.discarded], ['x'])

    def test_remaining_after_limited_pop_keeps_fifo_for_plain_pop(self):
        self.cache.push('d1', 'a', 100)
        self.cache.push('d2', 'b', 100)
        self.cache.push('d3', 'c', 100)
        r = self.cache.pop_live_batch(1)
        self.assertEqual(r.events, ['a'])
        self.assertEqual([self.cache.pop(), self.cache.pop()], ['b', 'c'])

    def test_limited_call_does_not_release_unscanned_slots(self):
        cache = self.make(3)
        cache.push_expiring('d1', 'x', 100, 1)
        cache.push('d2', 'a', 100)
        cache.push_expiring('d3', 'future', 100, 500)
        self.advance(10)
        r = cache.pop_live_batch(1)
        self.assertEqual(r.events, ['a'])
        self.assertEqual([d.event for d in r.discarded], ['x'])
        # 已扫描的两个槽位释放，未扫描的 future 仍占一个槽位
        self.assertEqual(cache.queue_status().size, 1)
        self.assertTrue(cache.push('d4', 'b', 100))
        self.assertTrue(cache.push('d5', 'c', 100))
        self.assertEqual(cache.push_with_reason('d6', 'full', 100).reason, 'queue_full')
        self.assertEqual(cache.pop_batch(), ['future', 'b', 'c'])

    # ---- values/seen 隔离与去重窗口保留 ----
    def test_does_not_touch_values_or_seen(self):
        self.cache.put('k', 'v', 100)
        self.cache.put('gone', 'g', 1)
        self.cache.push_expiring('d1', 'x', 100, 1)
        self.cache.push('d2', 'a', 100)
        seen_before = dict(self.cache.seen)
        self.advance(10)
        self.cache.pop_live_batch()
        self.assertEqual(self.cache.values['k'], ('v', 200))
        self.assertIn('gone', self.cache.values)  # 过期 value 不被顺带清理
        self.assertEqual(self.cache.seen, seen_before)  # 去重记录一律保留

    def test_discarded_event_frees_slot_but_dedupe_window_remains(self):
        cache = self.make(1)
        cache.push_expiring('d1', 'x', 100, 5)
        self.advance(10)  # 事件已到期；去重窗口（到 200）仍有效
        r = cache.pop_live_batch()
        self.assertEqual([d.event for d in r.discarded], ['x'])
        # 槽位已释放：不再 queue_full，但去重窗口优先拦截
        reason = cache.push_with_reason('d1', 'again', 100).reason
        self.assertEqual(reason, 'dedupe_window')
        self.assertEqual(cache.queue_status().size, 0)
        # 其他去重键可以使用释放出的槽位
        self.assertTrue(cache.push('d2', 'new', 100))

    def test_dedupe_of_live_event_also_remains(self):
        cache = self.make(2)
        cache.push_expiring('d1', 'x', 100, 1)
        cache.push('d2', 'a', 100)
        self.advance(10)
        cache.pop_live_batch()
        self.assertIn('d1', cache.seen)
        self.assertIn('d2', cache.seen)
        self.assertEqual(cache.push_with_reason('d2', 'dup', 100).reason, 'dedupe_window')

    # ---- 与普通出队接口共存 ----
    def test_plain_pop_and_pop_batch_still_ignore_clock(self):
        self.cache.push_expiring('d1', 'x', 100, 1)
        self.cache.push_expiring('d2', 'y', 100, 500)
        self.advance(10)
        self.clock_calls[0] = 0
        self.assertEqual(self.cache.pop(), 'x')  # 过期事件也原样返回
        self.assertEqual(self.cache.pop_batch(), ['y'])
        self.assertEqual(self.clock_calls[0], 0)

    def test_mixed_calls_compose(self):
        self.cache.push_expiring('d1', 'x', 100, 1)
        self.cache.push('d2', 'a', 100)
        self.cache.push_expiring('d3', 'y', 100, 1)
        self.cache.push('d4', 'b', 100)
        self.advance(10)
        r1 = self.cache.pop_live_batch(1)
        self.assertEqual((r1.events, [d.event for d in r1.discarded]), (['a'], ['x']))
        # 剩余 y(过期)、b：普通 pop_batch 不做过期判断，原样返回
        self.assertEqual(self.cache.pop_batch(), ['y', 'b'])

    # ---- limit 校验原子性 ----
    def test_invalid_limit_raises_without_clock_or_state_change(self):
        self.cache.push_expiring('d1', 'x', 100, 0)
        self.cache.push('d2', 'a', 100)
        self.advance(10)
        self.clock_calls[0] = 0
        for bad in (-1, -100, 1.5, 2.0, '3', [3], object(), True, False):
            with self.assertRaises(ValueError):
                self.cache.pop_live_batch(bad)
        self.assertEqual(self.clock_calls[0], 0)
        self.assertEqual(list(self.cache.events), ['x', 'a'])
        self.assertEqual(list(self.cache.event_expiries), [100, None])

    # ---- 时钟异常原样传播 ----
    def test_clock_exception_propagates_without_state_change(self):
        def bad_clock():
            raise RuntimeError('clock broken')

        cache = EventCache(bad_clock)
        # restore 不读取时钟，借此装入初始队列
        cache.restore({'values': {}, 'events': ['e1', 'e2'], 'seen': {}, 'max_queue': None})
        with self.assertRaises(RuntimeError):
            cache.pop_live_batch()
        self.assertEqual(list(cache.events), ['e1', 'e2'])  # 读时钟先于出队，状态不变

    # ---- 与快照恢复协作 ----
    def test_restored_expiries_drive_pop_live_batch(self):
        seed = EventCache(lambda: 100)
        seed.push_expiring('d1', 'x', 100, 5)
        seed.push('d2', 'a', 100)
        target = EventCache(lambda: 110)
        target.restore(seed.snapshot())
        r = target.pop_live_batch()
        self.assertEqual(r.events, ['a'])
        self.assertEqual([d.event for d in r.discarded], ['x'])

    def test_restored_old_format_events_always_live(self):
        cache = EventCache(lambda: 10 ** 9)
        cache.restore({'values': {}, 'events': ['a', 'b'], 'seen': {}, 'max_queue': None})
        r = cache.pop_live_batch()
        self.assertEqual(r.events, ['a', 'b'])
        self.assertEqual(r.discarded, [])


class SnapshotTest(unittest.TestCase):
    def setUp(self):
        self.now = [100]
        self.clock_calls = [0]

        def clock():
            self.clock_calls[0] += 1
            return self.now[0]

        self.clock = clock

    def make(self, max_queue=None, now=None):
        return EventCache(self.clock, max_queue=max_queue)

    def seed(self):
        cache = self.make(max_queue=3)
        cache.put('k1', 'v1', 50)   # 到期点 150
        cache.put('k2', 'v2', 200)  # 到期点 300
        cache.push('d1', 'e1', 10)  # seen d1 -> 110
        cache.push('d2', 'e2', 100)  # seen d2 -> 200
        return cache

    # ---- 快照结构与访问方式 ----
    def test_snapshot_has_core_fields_and_result_access(self):
        snap = self.seed().snapshot()
        self.assertIsInstance(snap, Result)
        # 回执状态始终随快照保存：seed 入队了 e1、e2，回执为 1、2，下一个为 3
        self.assertEqual(set(snap), {
            'values', 'events', 'seen', 'max_queue',
            'event_receipts', 'next_receipt', 'event_metadata'})
        self.assertEqual(snap.max_queue, 3)
        self.assertEqual(snap['max_queue'], 3)
        self.assertEqual(snap.events, ['e1', 'e2'])
        self.assertEqual(snap['events'], ['e1', 'e2'])
        self.assertEqual(snap.event_receipts, [1, 2])
        self.assertEqual(snap['event_receipts'], [1, 2])
        self.assertEqual(snap.next_receipt, 3)
        self.assertEqual(snap['next_receipt'], 3)
        self.assertEqual(snap.seen, {'d1': 110, 'd2': 200})
        self.assertEqual(snap['seen'], {'d1': 110, 'd2': 200})
        # values 与 dict.values 方法同名，属性访问仍须取到条目
        self.assertEqual(snap.values, {'k1': ('v1', 150), 'k2': ('v2', 300)})
        self.assertEqual(snap['values'], snap.values)

    def test_snapshot_events_is_plain_list(self):
        snap = self.seed().snapshot()
        self.assertIsInstance(snap.events, list)
        self.assertIsInstance(snap.event_receipts, list)

    # ---- 纯查询：不读时钟、不过滤过期项 ----
    def test_snapshot_does_not_read_clock_or_cleanup(self):
        cache = self.seed()
        self.now[0] = 130  # k1 与 d1 已到期
        calls_before = self.clock_calls[0]
        snap = cache.snapshot()
        self.assertEqual(self.clock_calls[0], calls_before)
        self.assertIn('k1', snap.values)  # 过期 value 原样保留
        self.assertIn('d1', snap.seen)    # 过期去重记录原样保留
        self.assertIn('k1', cache.values)
        self.assertIn('d1', cache.seen)
        self.assertEqual(snap.events, ['e1', 'e2'])

    # ---- 容器分离 ----
    def test_snapshot_is_detached_from_cache_both_ways(self):
        cache = self.seed()
        snap = cache.snapshot()
        snap['values']['k1'] = ('changed', 0)
        snap['events'].append('hack')
        snap['seen']['d1'] = 1
        self.assertEqual(cache.values['k1'], ('v1', 150))
        self.assertEqual(list(cache.events), ['e1', 'e2'])
        self.assertEqual(cache.seen['d1'], 110)

        cache.pop()
        cache.put('k3', 'v3', 10)
        self.assertEqual(snap.events, ['e1', 'e2', 'hack'])
        self.assertNotIn('k3', snap.values)

    def test_value_and_event_objects_keep_references(self):
        event = {'id': 1}
        value = object()
        cache = self.make()
        cache.put('k', value, 100)
        cache.push('d', event, 100)
        snap = cache.snapshot()
        self.assertIs(snap.values['k'][0], value)
        self.assertIs(snap.events[0], event)

    # ---- 恢复到另一时间源实例 ----
    def test_restore_into_other_clock_preserves_semantics(self):
        source = self.seed()
        self.now[0] = 130  # k1(150) 未到期、d1(110) 已到期
        snap = source.snapshot()

        target_now = [130]
        target = EventCache(lambda: target_now[0], max_queue=99)
        target.put('junk', 'j', 1000)
        target.push('zd', 'ze', 1000)
        self.assertIsNone(target.restore(snap))
        # 容量与状态全部来自快照，而非目标构造参数
        self.assertEqual(target.max_queue, 3)
        self.assertEqual(target.values, {'k1': ('v1', 150), 'k2': ('v2', 300)})
        self.assertEqual(target.seen, {'d1': 110, 'd2': 200})
        self.assertEqual([target.pop(), target.pop()], ['e1', 'e2'])

    def test_expiry_judged_by_target_clock_at_boundary(self):
        snap = self.seed().snapshot()
        target_now = [149]
        target = EventCache(lambda: target_now[0])
        target.restore(snap)
        self.assertEqual(target.get('k1'), 'v1')
        target_now[0] = 150  # 到期点 <= 当前时刻
        self.assertIsNone(target.get('k1'))
        self.assertNotIn('k1', target.values)

    def test_restored_capacity_and_reasons_match_saved_state(self):
        snap = self.seed().snapshot()
        target_now = [130]
        target = EventCache(lambda: target_now[0])
        target.restore(snap)
        # d1 到期点 110 <= 130：可重新入队，占最后一个槽位
        self.assertTrue(target.push('d1', 'e3', 10))
        self.assertEqual(target.push_with_reason('dx', 'full', 10).reason, 'queue_full')
        # d2 到期点 200 > 130：仍在去重窗口内，优先报 dedupe_window
        self.assertEqual(target.push_with_reason('d2', 'dup', 10).reason, 'dedupe_window')
        target_now[0] = 200  # 边界到期
        self.assertEqual(target.pop_batch(), ['e1', 'e2', 'e3'])  # 释放槽位
        self.assertTrue(target.push('d2', 'e4', 100))
        self.assertEqual(target.pop_batch(), ['e4'])

    def test_expired_dedupe_does_not_change_queued_order(self):
        snap = self.seed().snapshot()
        target = EventCache(lambda: 9999)  # 所有 seen 早已到期
        target.restore(snap)
        self.assertEqual(target.seen, {'d1': 110, 'd2': 200})  # 记录保留
        self.assertEqual(target.pop_batch(), ['e1', 'e2'])      # 顺序不动

    def test_restore_copies_containers_detached_from_snapshot(self):
        snap = self.seed().snapshot()
        target = EventCache(lambda: 0)
        target.restore(snap)
        snap['events'].append('nope')
        snap['values']['new'] = ('n', 1)
        self.assertEqual(list(target.events), ['e1', 'e2'])
        self.assertNotIn('new', target.values)

    def test_snapshot_restore_round_trip_is_equivalent(self):
        cache = self.seed()
        snap1 = cache.snapshot()
        rebuilt = EventCache(lambda: 0, max_queue=99)
        rebuilt.restore(snap1)
        snap2 = rebuilt.snapshot()
        self.assertEqual(snap1, snap2)

    def test_renewed_expires_at_survives_snapshot_restore(self):
        # 续期后的绝对到期点随 values 原样保存，快照格式无需新增字段
        cache = self.make()
        cache.put('a', 'v', 10)    # 110
        cache.put('n', None, 10)
        r = cache.renew('a', 40)   # 140
        self.assertEqual(r.expires_at, 140)
        cache.renew('n', 0)        # None 值 + 到期点 100
        snap = cache.snapshot()
        self.assertEqual(snap.values['a'], ('v', 140))
        self.assertEqual(snap.values['n'], (None, 100))
        rebuilt = EventCache(lambda: 130)
        rebuilt.restore(snap)
        self.assertEqual(rebuilt.values['a'], ('v', 140))  # 130 < 140：仍存活
        self.assertEqual(rebuilt.get('a'), 'v')
        self.assertEqual(rebuilt.values['n'], (None, 100))  # 已到期但不隐式清理
        # 旧四字段快照仍可恢复
        old = EventCache(lambda: 0)
        old.restore({'values': {'old': ('x', 999)}, 'events': [],
                     'seen': {}, 'max_queue': None})
        self.assertEqual(old.values, {'old': ('x', 999)})

    # ---- 恢复校验：失败原子、不读时钟 ----
    def assert_restore_rejected(self, cache, bad, exc_type):
        before = (dict(cache.values), list(cache.events), dict(cache.seen), cache.max_queue)
        calls_before = self.clock_calls[0]
        with self.assertRaises(exc_type):
            cache.restore(bad)
        self.assertEqual(self.clock_calls[0], calls_before)
        after = (dict(cache.values), list(cache.events), dict(cache.seen), cache.max_queue)
        self.assertEqual(before, after)

    def test_restore_requires_exactly_four_fields(self):
        cache = self.seed()
        good = {'values': {}, 'events': [], 'seen': {}, 'max_queue': None}
        for bad in (
            None, [], object(),
            {},
            {'values': {}, 'events': [], 'seen': {}},
            dict(good, extra=1),
            {'values': {}, 'seen': {}, 'max_queue': None},  # 缺 events
        ):
            self.assert_restore_rejected(cache, bad, ValueError)

    def test_restore_rejects_wrong_containers_and_pairs(self):
        cache = self.seed()
        for bad in (
            {'values': [], 'events': [], 'seen': {}, 'max_queue': None},
            {'values': {}, 'events': (), 'seen': {}, 'max_queue': None},
            {'values': {}, 'events': {}, 'seen': {}, 'max_queue': None},
            {'values': {}, 'events': [], 'seen': [], 'max_queue': None},
            {'values': {'k': 'v'}, 'events': [], 'seen': {}, 'max_queue': None},
            {'values': {'k': ('v',)}, 'events': [], 'seen': {}, 'max_queue': None},
            {'values': {'k': ('v', 1, 2)}, 'events': [], 'seen': {}, 'max_queue': None},
            {'values': {'k': ['v', 1]}, 'events': [], 'seen': {}, 'max_queue': None},
        ):
            self.assert_restore_rejected(cache, bad, ValueError)

    def test_restore_rejects_bad_max_queue(self):
        cache = self.seed()
        for mq in (-1, -10, 1.5, 2.0, True, False, '3', []):
            self.assert_restore_rejected(
                cache,
                {'values': {}, 'events': [], 'seen': {}, 'max_queue': mq},
                ValueError,
            )

    def test_restore_accepts_none_and_non_negative_int_max_queue(self):
        for mq in (None, 0, 1, 10 ** 6):
            cache = self.make()
            self.assertIsNone(
                cache.restore({'values': {}, 'events': [], 'seen': {}, 'max_queue': mq})
            )
            self.assertEqual(cache.max_queue, mq)

    def test_restore_accepts_mapping_subclass(self):
        from collections.abc import Mapping

        class M(Mapping):
            def __init__(self, data):
                self._data = data

            def __getitem__(self, key):
                return self._data[key]

            def __iter__(self):
                return iter(self._data)

            def __len__(self):
                return len(self._data)

        cache = self.make()
        good = {'values': {'k': ('v', 10)}, 'events': ['e'], 'seen': {}, 'max_queue': 2}
        self.assertIsNone(cache.restore(M(good)))
        self.assertEqual(cache.max_queue, 2)
        self.assertEqual(cache.pop_batch(), ['e'])

    def test_restore_unhashable_key_raises_type_error_atomically(self):
        from collections.abc import Mapping

        class OneItem(Mapping):
            # 绕过字典字面量构造期即抛出的限制，在解析时才暴露不可哈希键
            def __init__(self, key, value):
                self._key = key
                self._value = value

            def __getitem__(self, key):
                if key == self._key:
                    return self._value
                raise KeyError(key)

            def __iter__(self):
                yield self._key

            def __len__(self):
                return 1

        cache = self.seed()
        self.assert_restore_rejected(
            cache,
            {'values': OneItem(['k'], ('v', 1)), 'events': [], 'seen': {}, 'max_queue': None},
            TypeError,
        )
        self.assert_restore_rejected(
            cache,
            {'values': {}, 'events': [], 'seen': OneItem(['d'], 1), 'max_queue': None},
            TypeError,
        )

    def test_failed_restore_keeps_cache_fully_functional(self):
        cache = self.seed()
        with self.assertRaises(ValueError):
            cache.restore({'values': {}, 'events': [], 'seen': {}, 'max_queue': -1})
        self.now[0] = 100
        self.assertEqual(cache.get('k1'), 'v1')
        self.assertEqual(cache.pop(), 'e1')
        self.assertTrue(cache.push('d3', 'e3', 10))

    def test_none_values_and_events_survive_snapshot_restore(self):
        cache = self.make()
        cache.put('a', None, 100)
        cache.push(None, None, 100)
        target = EventCache(lambda: 100)
        target.restore(cache.snapshot())
        self.assertIsNone(target.get('a'))
        self.assertIn('a', target.values)
        self.assertEqual(target.pop_batch(), [None])

    def test_snapshot_preserves_none_value_and_expiry_delete_semantics(self):
        cache = self.make()
        cache.put('a', None, 50)    # 到期点 150
        snap = cache.snapshot()
        # None 值与其到期时间一并保留
        self.assertEqual(snap.values['a'], (None, 150))

        target = EventCache(lambda: 100)
        target.restore(snap)
        self.assertIn('a', target.values)
        self.assertTrue(target.delete('a'))       # 未到期的 None 值：删除成功
        self.assertFalse(target.delete('a'))
        self.assertIsNone(target.get('a'))

        expired = EventCache(lambda: 150)
        expired.restore(snap)
        self.assertIn('a', expired.values)
        self.assertTrue(expired.delete('a'))      # 已到期记录仍存在：同样删除成功
        self.assertFalse(expired.delete('a'))


class ExpiringEventsTest(unittest.TestCase):
    def setUp(self):
        self.now = [100]
        self.clock_calls = [0]

        def clock():
            self.clock_calls[0] += 1
            return self.now[0]

        self.clock = clock
        self.cache = EventCache(clock)

    def advance(self, seconds):
        self.now[0] += seconds

    def make(self, max_queue):
        return EventCache(self.clock, max_queue=max_queue)

    # ---- push_expiring 基本语义 ----
    def test_push_expiring_returns_true_and_fifo(self):
        self.assertTrue(self.cache.push_expiring('d1', 'e1', 10, 50))
        self.assertTrue(self.cache.push_expiring('d2', 'e2', 10, 50))
        self.assertEqual([self.cache.pop(), self.cache.pop()], ['e1', 'e2'])

    def test_push_expiring_with_reason_result_shape(self):
        r = self.cache.push_expiring_with_reason('d', 'e', 10, 50)
        self.assertIsInstance(r, Result)
        self.assertTrue(r.accepted)
        self.assertIsNone(r.reason)

    def test_push_expiring_reads_clock_exactly_once(self):
        self.cache.push_expiring('d', 'e', 10, 50)
        self.assertEqual(self.clock_calls[0], 1)

    def test_push_expiring_records_absolute_expiry(self):
        self.cache.push_expiring('d', 'e', 10, 50)
        self.assertEqual(list(self.cache.event_expiries), [150])

    def test_zero_event_ttl_expired_from_accept_moment(self):
        self.assertTrue(self.cache.push_expiring('d', 'e', 10, 0))
        # 不推进时钟：到期点 == 接受时刻，按 <= 边界已到期
        self.assertEqual(self.cache.cleanup_expired_events().events_removed, 1)
        self.assertIsNone(self.cache.pop())

    def test_event_ttl_validation_raises_before_clock_and_state(self):
        for bad in (-1, float('nan'), float('inf'), -float('inf'), '10', None, True, 1.0j):
            with self.assertRaises(ValueError):
                self.cache.push_expiring('d', 'e', 10, bad)
            with self.assertRaises(ValueError):
                self.cache.push_expiring_with_reason('d', 'e', 10, bad)
        self.assertEqual(self.clock_calls[0], 0)
        self.assertEqual(self.cache.queue_status().size, 0)
        self.assertEqual(self.cache.seen, {})

    def test_invalid_window_still_raises_value_error(self):
        for bad in (-1, float('nan'), True):
            with self.assertRaises(ValueError):
                self.cache.push_expiring('d', 'e', bad, 10)
        self.assertEqual(self.clock_calls[0], 0)

    def test_unhashable_dedupe_raises_type_error(self):
        with self.assertRaises(TypeError):
            self.cache.push_expiring(['d'], 'e', 10, 50)
        with self.assertRaises(TypeError):
            self.cache.push_expiring_with_reason(['d'], 'e', 10, 50)
        self.assertEqual(self.cache.queue_status().size, 0)
        self.assertEqual(self.cache.seen, {})

    # ---- 拒绝路径：与 push 一致的优先级与副作用 ----
    def test_dedupe_window_rejection_no_side_effects(self):
        self.cache.push_expiring('d', 'e1', 100, 50)
        expiry_before = self.cache.seen['d']
        r = self.cache.push_expiring_with_reason('d', 'e2', 100, 60)
        self.assertFalse(r.accepted)
        self.assertEqual(r.reason, 'dedupe_window')
        self.assertEqual(self.cache.seen['d'], expiry_before)  # 未延长
        self.assertEqual(self.cache.queue_status().size, 1)

    def test_queue_full_rejection_registers_nothing(self):
        cache = self.make(1)
        cache.push_expiring('d1', 'e1', 10, 50)
        r = cache.push_expiring_with_reason('d2', 'e2', 10, 50)
        self.assertFalse(r.accepted)
        self.assertEqual(r.reason, 'queue_full')
        self.assertNotIn('d2', cache.seen)
        self.assertEqual(list(cache.event_expiries), [150])

    def test_dedupe_window_takes_priority_over_queue_full(self):
        cache = self.make(1)
        cache.push_expiring('d1', 'e1', 100, 50)
        r = cache.push_expiring_with_reason('d1', 'dup', 100, 50)
        self.assertEqual(r.reason, 'dedupe_window')

    # ---- 生命周期：时间流逝不自动移除，清理前占容量、按 FIFO 出队 ----
    def test_expired_event_still_occupies_capacity(self):
        cache = self.make(1)
        cache.push_expiring('d1', 'e1', 10, 5)
        self.advance(10)  # 事件已到期
        r = cache.push_expiring_with_reason('d2', 'e2', 10, 5)
        self.assertFalse(r.accepted)
        self.assertEqual(r.reason, 'queue_full')  # 到期事件仍占槽位
        self.assertEqual(cache.cleanup_expired_events().events_removed, 1)
        self.assertTrue(cache.push_expiring('d2', 'e2', 10, 5))  # 清理后释放

    def test_pop_returns_expired_events_in_fifo_without_clock(self):
        self.cache.push_expiring('d1', 'e1', 10, 5)
        self.cache.push('d2', 'e2', 10)
        self.advance(100)  # e1 事件 TTL 到期
        self.clock_calls[0] = 0
        self.assertEqual([self.cache.pop(), self.cache.pop()], ['e1', 'e2'])
        self.assertEqual(self.clock_calls[0], 0)  # pop 不读取时钟

    def test_pop_batch_returns_expired_events_without_clock(self):
        self.cache.push_expiring('d1', 'e1', 10, 5)
        self.cache.push_expiring('d2', 'e2', 10, 500)
        self.advance(100)
        self.clock_calls[0] = 0
        self.assertEqual(self.cache.pop_batch(), ['e1', 'e2'])
        self.assertEqual(self.clock_calls[0], 0)
        self.assertEqual(list(self.cache.event_expiries), [])

    # ---- cleanup_expired_events ----
    def test_cleanup_expired_events_removes_only_expired_ttl_events(self):
        self.cache.push_expiring('d1', 'e1', 10, 5)    # 到期点 105
        self.cache.push('d2', 'e2', 10)                # 无 TTL
        self.cache.push_expiring('d3', 'e3', 10, 500)  # 到期点 600
        self.advance(10)  # 当前 110
        result = self.cache.cleanup_expired_events()
        self.assertEqual(result.events_removed, 1)
        self.assertEqual(self.cache.pop_batch(), ['e2', 'e3'])  # 顺序保留

    def test_cleanup_expired_events_boundary_inclusive(self):
        self.cache.push_expiring('d', 'e', 10, 5)  # 到期点 105
        self.advance(4)
        self.assertEqual(self.cache.cleanup_expired_events().events_removed, 0)
        self.advance(1)  # 恰达到期点
        self.assertEqual(self.cache.cleanup_expired_events().events_removed, 1)

    def test_cleanup_expired_events_reads_clock_once(self):
        self.cache.push_expiring('d', 'e', 10, 5)
        self.clock_calls[0] = 0
        self.cache.cleanup_expired_events()
        self.assertEqual(self.clock_calls[0], 1)

    def test_cleanup_expired_events_does_not_touch_values_or_seen(self):
        self.cache.put('k', 'v', 0)          # value 已到期
        self.cache.push_expiring('d', 'e', 0, 0)  # seen 与事件均已到期
        result = self.cache.cleanup_expired_events()
        self.assertEqual(result.events_removed, 1)
        self.assertIn('k', self.cache.values)  # values 不动
        self.assertIn('d', self.cache.seen)    # seen 不动

    def test_cleanup_expired_events_idempotent(self):
        self.cache.push_expiring('d', 'e', 10, 0)
        self.assertEqual(self.cache.cleanup_expired_events().events_removed, 1)
        self.assertEqual(self.cache.cleanup_expired_events().events_removed, 0)

    def test_cleanup_leaves_plain_cleanup_untouched(self):
        self.cache.push_expiring('d', 'e', 0, 0)  # seen 与事件均已到期
        result = self.cache.cleanup()  # 旧 cleanup 不清事件
        self.assertEqual((result.values_removed, result.dedupe_removed), (0, 1))
        self.assertEqual(self.cache.pop(), 'e')

    # ---- push_batch 四元组 ----
    def test_push_batch_accepts_mixed_triples_and_quadruples(self):
        results = self.cache.push_batch([
            ('d1', 'e1', 10),
            ('d2', 'e2', 10, 5),
            ['d3', 'e3', 10, 500],
        ])
        self.assertTrue(all(r.accepted for r in results))
        self.assertEqual(list(self.cache.event_expiries), [None, 105, 600])
        self.assertEqual(self.clock_calls[0], 1)

    def test_push_batch_quadruple_expiry_uses_batch_moment(self):
        self.advance(7)
        self.cache.push_batch([('d1', 'e1', 10, 5), ('d2', 'e2', 10, 5)])
        self.assertEqual(list(self.cache.event_expiries), [112, 112])

    def test_push_batch_invalid_event_ttl_is_atomic(self):
        self.cache.put('k', 'v', 100)
        for bad in (-1, float('nan'), float('inf'), True, '5', None):
            with self.assertRaises(ValueError):
                self.cache.push_batch([('d1', 'e1', 10), ('d2', 'e2', 10, bad)])
        self.assertEqual(self.clock_calls[0], 1)  # 只有 put 读过一次
        self.assertEqual(self.cache.queue_status().size, 0)
        self.assertEqual(self.cache.seen, {})

    def test_push_batch_wrong_arity_still_rejected(self):
        for bad in ([('d', 'e')], [('d', 'e', 10, 5, 'x')], [()], ['abc']):
            with self.assertRaises(ValueError):
                self.cache.push_batch(bad)
        self.assertEqual(self.clock_calls[0], 0)

    def test_push_batch_unhashable_dedupe_with_ttl_raises_type_error(self):
        with self.assertRaises(TypeError):
            self.cache.push_batch([('d1', 'e1', 10, 5), (['d2'], 'e2', 10, 5)])
        self.assertEqual(self.clock_calls[0], 0)
        self.assertEqual(self.cache.queue_status().size, 0)

    # ---- apply_batch push_expiring ----
    def test_apply_batch_push_expiring_operation(self):
        results = self.cache.apply_batch([
            ('push_expiring', 'd1', 'e1', 10, 5),
            ('push', 'd2', 'e2', 10),
            ('push_expiring', 'd1', 'dup', 10, 5),
        ])
        self.assertEqual([(r.accepted, r.reason) for r in results], [
            (True, None), (True, None), (False, 'dedupe_window'),
        ])
        self.assertEqual(list(self.cache.event_expiries), [105, None])
        self.assertEqual(self.clock_calls[0], 1)

    def test_apply_batch_push_expiring_validation_atomic(self):
        for bad, exc in (
            ([('push_expiring', 'd', 'e', 10)], ValueError),          # 缺 event_ttl
            ([('push_expiring', 'd', 'e', 10, 5, 'x')], ValueError),  # 多余成员
            ([('push_expiring', 'd', 'e', 10, -1)], ValueError),
            ([('push_expiring', 'd', 'e', 10, None)], ValueError),
            ([('push_expiring', 'd', 'e', -1, 5)], ValueError),
            ([('push_expiring', ['d'], 'e', 10, 5)], TypeError),
        ):
            calls_before = self.clock_calls[0]
            with self.assertRaises(exc):
                self.cache.apply_batch(bad)
            self.assertEqual(self.clock_calls[0], calls_before)
        self.assertEqual(self.cache.queue_status().size, 0)
        self.assertEqual(self.cache.seen, {})


class ExpiringSnapshotTest(unittest.TestCase):
    def setUp(self):
        self.now = [100]
        self.clock_calls = [0]

        def clock():
            self.clock_calls[0] += 1
            return self.now[0]

        self.clock = clock
        self.cache = EventCache(clock)

    def advance(self, seconds):
        self.now[0] += seconds

    # ---- 快照格式 ----
    def test_snapshot_without_ttl_events_keeps_core_and_receipt_fields(self):
        self.cache.push('d', 'e', 10)
        snap = self.cache.snapshot()
        self.assertEqual(set(snap), {
            'values', 'events', 'seen', 'max_queue',
            'event_receipts', 'next_receipt', 'event_metadata'})
        self.assertEqual(snap.event_receipts, [1])
        self.assertEqual(snap.next_receipt, 2)

    def test_snapshot_with_ttl_events_adds_aligned_expiries(self):
        self.cache.push('d1', 'e1', 10)
        self.cache.push_expiring('d2', 'e2', 10, 50)
        self.cache.push_expiring('d3', 'e3', 10, 5)
        snap = self.cache.snapshot()
        self.assertEqual(set(snap), {
            'values', 'events', 'seen', 'max_queue', 'event_expiries',
            'event_receipts', 'next_receipt', 'event_metadata'})
        self.assertEqual(snap.events, ['e1', 'e2', 'e3'])
        self.assertEqual(snap['event_expiries'], [None, 150, 105])
        self.assertEqual(snap.event_expiries, [None, 150, 105])  # 属性访问可用
        self.assertIsInstance(snap.event_expiries, list)
        # 回执与事件对齐：三次接受分配 1、2、3，下一个为 4
        self.assertEqual(snap.event_receipts, [1, 2, 3])
        self.assertEqual(snap.next_receipt, 4)

    def test_snapshot_expiries_detached_from_cache(self):
        self.cache.push_expiring('d', 'e', 10, 50)
        snap = self.cache.snapshot()
        snap['event_expiries'][0] = 999
        self.assertEqual(list(self.cache.event_expiries), [150])

    def test_snapshot_after_cleanup_of_all_ttl_events_drops_expiries_but_keeps_history(self):
        self.cache.push_expiring('d', 'e', 10, 0)
        self.cache.push('d2', 'plain', 10)
        self.cache.cleanup_expired_events()
        snap = self.cache.snapshot()
        # 所有带 TTL 事件清空后不再有 event_expiries 字段……
        self.assertNotIn('event_expiries', snap)
        # ……但清理动作写入了丢弃历史，按新规格历史非空时保留历史字段；
        # 回执状态始终保留：e 被清理、plain 在队（回执 2），下一个回执为 3
        self.assertEqual(set(snap), {
            'values', 'events', 'seen', 'max_queue',
            'discard_history', 'discard_history_limit',
            'event_receipts', 'next_receipt', 'event_metadata',
        })
        self.assertEqual(snap.events, ['plain'])
        self.assertEqual(snap.event_receipts, [2])
        self.assertEqual(snap.next_receipt, 3)
        self.assertEqual(len(snap.discard_history), 1)
        entry = snap.discard_history[0]
        self.assertEqual((entry.event, entry.reason), ('e', 'event_ttl'))
        self.assertIsInstance(entry.timestamp, (int, float))
        self.assertIsNone(snap.discard_history_limit)

    # ---- restore 新格式 ----
    def test_restore_new_format_round_trip(self):
        self.cache.put('k', 'v', 50)
        self.cache.push('d1', 'e1', 10)
        self.cache.push_expiring('d2', 'e2', 10, 50)
        snap1 = self.cache.snapshot()
        target = EventCache(self.clock, max_queue=99)
        self.assertIsNone(target.restore(snap1))
        self.assertEqual(list(target.event_expiries), [None, 150])
        self.assertEqual(target.snapshot(), snap1)

    def test_restored_expiries_drive_cleanup_on_target_clock(self):
        self.cache.push_expiring('d', 'e1', 10, 50)  # 到期点 150
        self.cache.push('d2', 'e2', 10)
        snap = self.cache.snapshot()
        target_now = [160]
        target = EventCache(lambda: target_now[0])
        target.restore(snap)
        self.assertEqual(target.cleanup_expired_events().events_removed, 1)
        self.assertEqual(target.pop_batch(), ['e2'])

    def test_restore_old_format_gives_no_expiries(self):
        self.cache.restore({'values': {}, 'events': ['a', 'b'], 'seen': {}, 'max_queue': None})
        self.assertEqual(list(self.cache.event_expiries), [None, None])
        # 旧格式事件无回执（全 None），新回执从 1 开始分配
        self.assertEqual(list(self.cache.event_receipts), [None, None])
        self.assertEqual(self.cache.snapshot().next_receipt, 1)
        # 旧事件 cancel 不命中；此后接受的新事件拿到回执 1
        self.assertEqual(self.cache.cancel(1),
                         Result(removed=False, event=None, reason='missing'))
        r = self.cache.push_with_receipt('d', 'c', 10)
        self.assertEqual((r.receipt, r.accepted, r.reason), (1, True, None))
        # 恢复后再快照：回执两字段始终出现，旧事件仍以 None 对齐
        self.assertEqual(set(self.cache.snapshot()), {
            'values', 'events', 'seen', 'max_queue',
            'event_receipts', 'next_receipt', 'event_metadata'})

    def test_restore_copies_expiries_detached_from_snapshot(self):
        self.cache.push_expiring('d', 'e', 10, 50)
        snap = self.cache.snapshot()
        target = EventCache(self.clock)
        target.restore(snap)
        snap['event_expiries'][0] = 1
        self.assertEqual(list(target.event_expiries), [150])

    # ---- restore 校验：失败原子 ----
    def assert_restore_rejected(self, bad):
        self.cache.put('k', 'v', 100)
        self.cache.push_expiring('d', 'e', 100, 50)
        before = (dict(self.cache.values), list(self.cache.events),
                  list(self.cache.event_expiries), dict(self.cache.seen))
        calls_before = self.clock_calls[0]
        with self.assertRaises(ValueError):
            self.cache.restore(bad)
        self.assertEqual(self.clock_calls[0], calls_before)  # 未读取时钟
        after = (dict(self.cache.values), list(self.cache.events),
                 list(self.cache.event_expiries), dict(self.cache.seen))
        self.assertEqual(before, after)

    def test_restore_rejects_mismatched_expiries_length(self):
        base = {'values': {}, 'events': ['a'], 'seen': {}, 'max_queue': None}
        self.assert_restore_rejected(dict(base, event_expiries=[]))
        self.assert_restore_rejected(dict(base, event_expiries=[None, None]))

    def test_restore_rejects_bad_expiries_container_and_entries(self):
        base = {'values': {}, 'events': ['a'], 'seen': {}, 'max_queue': None}
        for bad_expiries in (
            (None,),           # 非列表容器
            'x',               # 非列表
            [float('nan')],
            [float('inf')],
            ['150'],
            [True],
            [(1, 2)],
        ):
            self.assert_restore_rejected(dict(base, event_expiries=bad_expiries))

    def test_restore_rejects_unknown_extra_field(self):
        self.assert_restore_rejected(
            {'values': {}, 'events': [], 'seen': {}, 'max_queue': None, 'bogus': 1})

    def test_restore_accepts_none_and_numeric_expiries(self):
        cache = EventCache(self.clock)
        cache.restore({
            'values': {}, 'events': ['a', 'b', 'c'], 'seen': {}, 'max_queue': None,
            'event_expiries': [None, 150, 2.5],
        })
        self.assertEqual(list(cache.event_expiries), [None, 150, 2.5])


class ReplayBatchTest(unittest.TestCase):
    def setUp(self):
        self.now = [100]
        self.clock_calls = [0]

        def clock():
            self.clock_calls[0] += 1
            return self.now[0]

        self.clock = clock
        self.cache = EventCache(clock)

    def advance(self, seconds):
        self.now[0] += seconds

    def make(self, max_queue):
        return EventCache(self.clock, max_queue=max_queue)

    # ---- 基本语义：记录时刻即当前时刻 ----
    def test_empty_records_returns_empty_without_clock(self):
        self.assertEqual(self.cache.replay_batch([]), [])
        self.assertEqual(self.clock_calls[0], 0)

    def test_replay_never_reads_injected_clock(self):
        results = self.cache.replay_batch([
            (10, ('put', 'a', 'v', 5)),
            (20, ('push', 'd1', 'e1', 10)),
            (25, ('push_expiring', 'd2', 'e2', 10, 5)),
            (30, ('cleanup',)),
            (30, ('cleanup_expired_events',)),
            (40, ('delete', 'a')),
        ])
        self.assertEqual(self.clock_calls[0], 0)
        self.assertEqual([set(r) for r in results], [
            {'accepted', 'reason'},
            {'accepted', 'reason'},
            {'accepted', 'reason'},
            {'values_removed', 'dedupe_removed'},
            {'events_removed'},
            {'deleted'},
        ])

    def test_put_uses_record_timestamp_for_expiry(self):
        self.cache.replay_batch([(10, ('put', 'a', 'v', 5))])
        self.assertEqual(self.cache.values['a'], ('v', 15))
        # 记录时间不推进注入时钟：按当前时钟 100 判定已到期
        self.assertIsNone(self.cache.get('a'))

    def test_delete_result_matches_single_delete(self):
        self.cache.replay_batch([(10, ('put', 'n', None, 100))])
        results = self.cache.replay_batch([(20, ('delete', 'n')), (20, ('delete', 'n'))])
        self.assertEqual([r.deleted for r in results], [True, False])
        self.assertNotIn('n', self.cache.values)

    # ---- renew 标签：以记录时刻计算绝对到期点 ----
    def test_renew_uses_record_timestamp_and_never_reads_clock(self):
        results = self.cache.replay_batch([
            (100, ('put', 'a', 'v', 10)),   # 到期点 110
            (105, ('renew', 'a', 20)),      # 105 + 20 = 125
        ])
        self.assertEqual(self.clock_calls[0], 0)
        r = results[1]
        self.assertEqual(set(r), {'renewed', 'value', 'reason', 'expires_at'})
        self.assertTrue(r.renewed)
        self.assertEqual(r.value, 'v')
        self.assertIsNone(r.reason)
        self.assertEqual(r.expires_at, 125)
        self.assertEqual(self.cache.values['a'], ('v', 125))

    def test_renew_boundary_expired_and_missing_in_replay(self):
        results = self.cache.replay_batch([
            (100, ('put', 'a', 'v', 10)),   # 到期点 110
            (110, ('renew', 'a', 5)),       # 到期点 <= 记录时刻：expired 并删除
            (111, ('renew', 'a', 5)),       # 已删除：missing
        ])
        self.assertFalse(results[1].renewed)
        self.assertIsNone(results[1].value)
        self.assertEqual(results[1].reason, 'expired')
        self.assertIsNone(results[1].expires_at)
        self.assertEqual(results[2].reason, 'missing')
        self.assertNotIn('a', self.cache.values)

    def test_renew_preserves_falsy_values_in_replay(self):
        results = self.cache.replay_batch([
            (0, ('put', 'n', None, 10)),
            (0, ('put', 'f', False, 10)),
            (0, ('put', 'z', 0, 10)),
            (5, ('renew', 'n', 10)),
            (5, ('renew', 'f', 10)),
            (5, ('renew', 'z', 10)),
        ])
        for i, value in ((3, None), (4, False), (5, 0)):
            self.assertTrue(results[i].renewed)
            self.assertIs(results[i].value, value)
            self.assertEqual(results[i].expires_at, 15)
        self.assertEqual(self.cache.values['n'], (None, 15))
        self.assertEqual(self.cache.values['f'], (False, 15))
        self.assertEqual(self.cache.values['z'], (0, 15))

    def test_renew_replay_is_deterministic_across_runs(self):
        records = [
            (0, ('put', 'k', 'payload', 10)),
            (5, ('renew', 'k', 100)),
            (10, ('renew', 'k', 1)),
            (11, ('get_with_reason', 'k')),
        ]

        def run():
            cache = EventCache(lambda: 999999)  # 永不被读取
            return cache.replay_batch(records), dict(cache.values)

        self.assertEqual(run(), run())
        results, _values = run()
        self.assertEqual(results[1].expires_at, 105)
        self.assertEqual(results[2].expires_at, 11)
        self.assertEqual(results[3], {'found': False, 'value': None, 'reason': 'expired'})

    def test_renew_in_replay_does_not_touch_seen_or_events(self):
        self.cache.replay_batch([
            (0, ('put', 'k', 'v', 10)),
            (0, ('push', 'd', 'e', 100)),   # seen['d'] 到期点 100
            (5, ('renew', 'k', 50)),        # k -> 55
            (50, ('renew', 'k', 151)),      # k -> 201；此刻 seen['d'] 已到期但不被清理
            (200, ('renew', 'k', 1)),       # k（201 > 200）仍存活 -> 201；seen 仍不动
        ])
        self.assertEqual(self.cache.seen['d'], 100)
        self.assertEqual(list(self.cache.events), ['e'])
        self.assertEqual(self.cache.values['k'], ('v', 201))

    def test_invalid_renew_record_raises_atomically(self):
        self.cache.put('keep', 'v', 1000)
        for bad, exc in (
            ([(0, ('renew', 'x'))], ValueError),
            ([(0, ('renew', 'x', 1, 2))], ValueError),
            ([(0, ('renew', 'x', -1))], ValueError),
            ([(0, ('renew', 'x', True))], ValueError),
            ([(0, ('renew', 'x', float('nan')))], ValueError),
            ([(0, ('renew', 'keep', 100)), (0, ('renew', 'x', -1))], ValueError),
            ([(5, ('renew', 'x', 1)), (4, ('renew', 'x', 1))], ValueError),
            ([(0, ('renew', ['unhashable'], 1))], TypeError),
        ):
            self.assert_replay_rejected(bad, exc)
        # 校验失败不产生部分写入：keep 的到期点仍为 put 时的 1100
        self.assertEqual(self.cache.values['keep'], ('v', 1100))

    def test_push_results_only_accepted_and_reason(self):
        results = self.cache.replay_batch([
            (10, ('push', 'd', 'e1', 10)),
            (15, ('push', 'd', 'e2', 10)),   # 窗口 10+10=20 > 15：去重拒绝
            (20, ('push', 'd', 'e3', 10)),   # 到期点 <= 记录时刻：可重入
        ])
        self.assertEqual([(r.accepted, r.reason) for r in results], [
            (True, None), (False, 'dedupe_window'), (True, None),
        ])
        self.assertEqual(self.cache.pop_batch(), ['e1', 'e3'])

    def test_same_timestamp_shares_boundary(self):
        results = self.cache.replay_batch([
            (10, ('push', 'd', 'e1', 0)),   # window=0：到期点 == 记录时刻
            (10, ('push', 'd', 'e2', 0)),   # 同一时刻按 <= 边界可重入
        ])
        self.assertTrue(all(r.accepted for r in results))
        self.assertEqual(self.cache.pop_batch(), ['e1', 'e2'])

    def test_push_expiring_records_absolute_expiry_from_record_time(self):
        self.cache.replay_batch([(10, ('push_expiring', 'd', 'e', 10, 5))])
        self.assertEqual(list(self.cache.event_expiries), [15])

    def test_queue_full_rejection_uses_record_moment_state(self):
        cache = self.make(1)
        results = cache.replay_batch([
            (10, ('push', 'd1', 'e1', 10)),
            (11, ('push', 'd2', 'e2', 10)),
        ])
        self.assertEqual([(r.accepted, r.reason) for r in results], [
            (True, None), (False, 'queue_full'),
        ])
        self.assertNotIn('d2', cache.seen)

    def test_time_advance_does_not_auto_cleanup(self):
        # 记录时刻前进本身不清理 values/seen/事件；需显式 cleanup 操作
        self.cache.replay_batch([
            (10, ('put', 'a', 'v', 5)),
            (10, ('push_expiring', 'd', 'e', 5, 5)),
            (100, ('push', 'd2', 'e2', 5)),
        ])
        self.assertIn('a', self.cache.values)
        self.assertIn('d', self.cache.seen)
        self.assertEqual(self.cache.queue_status().size, 2)
        results = self.cache.replay_batch([
            (100, ('cleanup',)),
            (100, ('cleanup_expired_events',)),
        ])
        self.assertEqual((results[0].values_removed, results[0].dedupe_removed), (1, 1))
        self.assertEqual(results[1].events_removed, 1)
        self.assertEqual(self.cache.pop_batch(), ['e2'])

    def test_cleanup_expired_events_boundary_inclusive(self):
        self.cache.replay_batch([
            (10, ('push_expiring', 'd', 'e', 10, 5)),  # 到期点 15
            (14, ('cleanup_expired_events',)),
        ])
        self.assertEqual(self.cache.queue_status().size, 1)
        results = self.cache.replay_batch([(15, ('cleanup_expired_events',))])
        self.assertEqual(results[0].events_removed, 1)
        self.assertIsNone(self.cache.pop())

    def test_accepts_generator_and_list_records(self):
        gen = (record for record in [(1, ('put', 'a', 1, 10)), (2, ('delete', 'a'))])
        results = self.cache.replay_batch(gen)
        self.assertTrue(results[0].accepted)
        self.assertTrue(results[1].deleted)

    # ---- 校验：失败原子、不读时钟 ----
    def assert_replay_rejected(self, records, exc_type):
        before = (dict(self.cache.values), list(self.cache.events),
                  list(self.cache.event_expiries), dict(self.cache.seen))
        calls_before = self.clock_calls[0]
        with self.assertRaises(exc_type):
            self.cache.replay_batch(records)
        self.assertEqual(self.clock_calls[0], calls_before)
        after = (dict(self.cache.values), list(self.cache.events),
                 list(self.cache.event_expiries), dict(self.cache.seen))
        self.assertEqual(before, after)

    def test_non_iterable_records_raise_value_error(self):
        for bad in (None, 42, 3.14, True):
            self.assert_replay_rejected(bad, ValueError)

    def test_record_not_pair_raises_value_error(self):
        for bad in (
            [(10,)],
            [(10, ('cleanup',), 'extra')],
            [()],
            [42],
            ['ab'],
        ):
            self.assert_replay_rejected(bad, ValueError)

    def test_invalid_timestamp_raises_value_error(self):
        for bad_ts in (True, False, float('nan'), float('inf'), -float('inf'), '10', None, 1.0j):
            self.assert_replay_rejected([(bad_ts, ('cleanup',))], ValueError)

    def test_time_regression_raises_value_error(self):
        self.assert_replay_rejected(
            [(10, ('put', 'a', 'v', 5)), (9, ('put', 'b', 'v', 5))], ValueError)
        self.assertEqual(self.cache.values, {})

    def test_negative_and_float_timestamps_accepted(self):
        results = self.cache.replay_batch([
            (-5, ('put', 'a', 'v', 10)),   # 到期点 5
            (-5, ('push', 'd', 'e', 10)),
            (2.5, ('cleanup',)),           # 5 > 2.5：未到期，不清理
        ])
        self.assertTrue(all(r is not None for r in results))
        self.assertEqual(results[2].values_removed, 0)
        self.assertIn('a', self.cache.values)
        results = self.cache.replay_batch([(5, ('cleanup',))])  # 到期点 <= 记录时刻
        self.assertEqual(results[0].values_removed, 1)
        self.assertEqual(self.cache.values, {})

    def test_invalid_operation_raises_value_error_atomically(self):
        for bad in (
            [(1, ('put', 'a', 'v', 10)), (2, 'not-a-tuple')],
            [(1, ('put', 'a', 'v', 10)), (2, ())],
            [(1, ('put', 'a', 'v', 10)), (2, ('unknown',))],
            [(1, ('put', 'a', 'v', 10)), (2, ('put', 'a', 'v'))],
            [(1, ('put', 'a', 'v', 10)), (2, ('put', 'a', 'v', -1))],
            [(1, ('put', 'a', 'v', 10)), (2, ('push', 'd', 'e', float('nan')))],
            [(1, ('put', 'a', 'v', 10)), (2, ('push_expiring', 'd', 'e', 10, True))],
            [(1, ('put', 'a', 'v', 10)), (2, ('cleanup', 'x'))],
            [(1, ('put', 'a', 'v', 10)), (2, ('cleanup_expired_events', 'x'))],
        ):
            self.assert_replay_rejected(bad, ValueError)
        self.assertEqual(self.cache.values, {})

    def test_unhashable_key_or_dedupe_raises_type_error_atomically(self):
        for bad in (
            [(1, ('put', ['k'], 'v', 10))],
            [(1, ('put', 'a', 'v', 10)), (2, ('delete', ['k']))],
            [(1, ('push', ['d'], 'e', 10))],
            [(1, ('put', 'a', 'v', 10)), (2, ('push_expiring', {'d': 1}, 'e', 10, 5))],
        ):
            self.assert_replay_rejected(bad, TypeError)
        self.assertEqual(self.cache.values, {})
        self.assertEqual(self.cache.seen, {})

    def test_error_precedence_follows_record_order(self):
        # 先出现时间倒退：ValueError 优先于后项的 TypeError
        self.assert_replay_rejected(
            [(10, ('cleanup',)), (5, ('put', ['k'], 'v', 1))], ValueError)
        # 先出现不可哈希 key：TypeError 优先于后项的 ValueError
        self.assert_replay_rejected(
            [(1, ('put', ['k'], 'v', 1)), (2, ('put', 'a', 'v', -1))], TypeError)

    def test_later_invalid_record_rolls_back_whole_batch(self):
        cache = self.make(1)
        with self.assertRaises(ValueError):
            cache.replay_batch([
                (1, ('push_expiring', 'd', 'e', 10, 5)),
                (2, ('push', 'd2', 'e2', -1)),
            ])
        self.assertEqual(cache.queue_status().size, 0)
        self.assertEqual(cache.seen, {})
        self.assertEqual(list(cache.event_expiries), [])

    def test_failed_replay_preserves_existing_state(self):
        self.cache.put('k', 'v', 100)
        self.cache.push('d0', 'e0', 100)
        calls_before = self.clock_calls[0]
        with self.assertRaises(ValueError):
            self.cache.replay_batch([(1, ('put', 'a', 'v', 10)), (2, ('delete',))])
        self.assertEqual(self.clock_calls[0], calls_before)
        self.assertEqual(self.cache.get('k'), 'v')
        self.assertEqual(self.cache.pop(), 'e0')

    # ---- 与快照/恢复协作 ----
    def test_replayed_expiries_survive_snapshot_restore(self):
        self.cache.replay_batch([
            (10, ('put', 'a', 'v', 5)),
            (10, ('push_expiring', 'd', 'e', 10, 50)),
        ])
        snap = self.cache.snapshot()
        self.assertEqual(snap.values['a'], ('v', 15))
        self.assertEqual(snap['event_expiries'], [60])
        target = EventCache(self.clock)
        target.restore(snap)
        self.assertEqual(target.values['a'], ('v', 15))
        self.assertEqual(list(target.event_expiries), [60])
        self.assertEqual(target.snapshot(), snap)

    def test_old_format_snapshot_still_restorable_after_replay(self):
        self.cache.replay_batch([(10, ('push', 'd', 'e', 10))])
        target = EventCache(self.clock)
        target.restore({'values': {}, 'events': ['a'], 'seen': {}, 'max_queue': None})
        self.assertEqual(list(target.event_expiries), [None])
        self.assertEqual(target.pop_batch(), ['a'])

    # ---- 读取/出队记录：不读注入时钟 ----
    def test_read_records_never_read_injected_clock(self):
        self.cache.replay_batch([
            (0, ('put', 'a', 'v', 10)),
            (0, ('push', 'd1', 'e1', 10)),
            (5, ('get', 'a')),
            (5, ('get_with_reason', 'a')),
            (5, ('pop',)),
            (5, ('pop_batch',)),
            (5, ('peek',)),
            (5, ('pop_live_batch',)),
            (5, ('peek_live_batch',)),
            (5, ('queue_status',)),
        ])
        self.assertEqual(self.clock_calls[0], 0)

    def test_read_results_correspond_one_to_one_with_records(self):
        results = self.cache.replay_batch([
            (0, ('get', 'missing')),
            (0, ('pop',)),
            (0, ('pop_batch',)),
            (0, ('peek',)),
            (0, ('pop_live_batch',)),
            (0, ('peek_live_batch',)),
            (0, ('queue_status',)),
        ])
        self.assertEqual(len(results), 7)
        self.assertIsNone(results[0])          # get 缺失键
        self.assertIsNone(results[1])          # pop 空队列
        self.assertEqual(results[2], [])       # pop_batch 空队列
        self.assertEqual(results[3], [])       # peek 空队列
        self.assertEqual((results[4].events, results[4].discarded), ([], []))
        self.assertEqual((results[5].events, results[5].discarded), ([], []))
        self.assertEqual((results[6].size, results[6].max_queue), (0, None))

    # ---- get 记录：按记录时刻判定键值 TTL ----
    def test_replay_get_uses_record_timestamp_for_ttl(self):
        results = self.cache.replay_batch([
            (10, ('put', 'a', 'v', 5)),       # 到期点 15
            (14, ('get', 'a')),               # 14 < 15：仍可读
            (15, ('get', 'a')),               # 到期点 <= 记录时刻：None 并移除
            (16, ('get', 'a')),               # 已被移除：None
        ])
        self.assertEqual(results[1:], ['v', None, None])
        self.assertNotIn('a', self.cache.values)

    def test_replay_get_same_timestamp_zero_ttl_is_expired(self):
        results = self.cache.replay_batch([
            (10, ('put', 'a', 'v', 0)),       # 到期点 == 记录时刻
            (10, ('get', 'a')),
        ])
        self.assertIsNone(results[1])
        self.assertNotIn('a', self.cache.values)

    def test_replay_get_missing_key_returns_none(self):
        results = self.cache.replay_batch([(10, ('get', 'x'))])
        self.assertIsNone(results[0])

    def test_replay_get_falsy_live_value_preserved(self):
        results = self.cache.replay_batch([
            (10, ('put', 'n', None, 100)),
            (10, ('put', 'z', 0, 100)),
            (10, ('get', 'n')),
            (10, ('get', 'z')),
        ])
        self.assertIsNone(results[2])          # None 是存活值，不是缺失
        self.assertIn('n', self.cache.values)
        self.assertEqual(results[3], 0)

    def test_replay_get_removal_is_observable_by_later_records(self):
        results = self.cache.replay_batch([
            (10, ('put', 'a', 'v', 0)),
            (20, ('get', 'a')),                # 到期移除
            (20, ('delete', 'a')),             # 已不存在 -> False
            (20, ('get', 'a')),
        ])
        self.assertIsNone(results[1])
        self.assertFalse(results[2].deleted)
        self.assertIsNone(results[3])

    # ---- get_with_reason 记录：按记录时刻判定、三态、不读注入时钟 ----
    def test_replay_get_with_reason_three_states(self):
        results = self.cache.replay_batch([
            (10, ('put', 'a', 'v', 5)),        # 到期点 15
            (10, ('put', 'n', None, 100)),
            (10, ('put', 'f', False, 100)),
            (14, ('get_with_reason', 'a')),   # 存活
            (14, ('get_with_reason', 'n')),   # None 假值仍 found
            (14, ('get_with_reason', 'f')),   # False 假值仍 found
            (14, ('get_with_reason', 'x')),   # 缺失
            (15, ('get_with_reason', 'a')),   # 到期点 <= 记录时刻：expired 并移除
            (16, ('get_with_reason', 'a')),   # 已移除：missing
        ])
        self.assertEqual(self.clock_calls[0], 0)  # 全程不读注入时钟
        self.assertEqual(dict(results[3]), {'found': True, 'value': 'v', 'reason': None})
        self.assertEqual(dict(results[4]), {'found': True, 'value': None, 'reason': None})
        self.assertEqual(dict(results[5]), {'found': True, 'value': False, 'reason': None})
        self.assertEqual(dict(results[6]), {'found': False, 'value': None, 'reason': 'missing'})
        self.assertEqual(dict(results[7]), {'found': False, 'value': None, 'reason': 'expired'})
        self.assertEqual(dict(results[8]), {'found': False, 'value': None, 'reason': 'missing'})
        self.assertNotIn('a', self.cache.values)

    def test_replay_get_with_reason_same_timestamp_zero_ttl_is_expired(self):
        results = self.cache.replay_batch([
            (10, ('put', 'a', 'v', 0)),
            (10, ('get_with_reason', 'a')),    # 到期点 == 记录时刻
            (10, ('get_with_reason', 'a')),    # 同刻后续记录视为 missing
        ])
        self.assertEqual(results[1].reason, 'expired')
        self.assertEqual(results[2].reason, 'missing')
        self.assertNotIn('a', self.cache.values)

    def test_replay_get_with_reason_does_not_touch_queue_or_seen(self):
        results = self.cache.replay_batch([
            (10, ('put', 'a', 'v', 0)),
            (10, ('push', 'd', 'e', 0)),
            (10, ('push_expiring', 'd2', 'ex', 100, 0)),
            (100, ('get_with_reason', 'a')),   # expired，只移除该值键
        ])
        self.assertEqual(results[3].reason, 'expired')
        self.assertNotIn('a', self.cache.values)
        self.assertEqual(set(self.cache.seen), {'d', 'd2'})
        self.assertEqual(list(self.cache.events), ['e', 'ex'])
        self.assertEqual(list(self.cache.event_expiries), [None, 10])

    def test_replay_get_with_reason_results_correspond_one_to_one(self):
        results = self.cache.replay_batch([
            (0, ('get_with_reason', 'x')),
            (0, ('get_with_reason', 'y')),
            (0, ('put', 'z', 1, 10)),
            (0, ('get_with_reason', 'z')),
        ])
        self.assertEqual(len(results), 4)
        for r in (results[0], results[1], results[3]):
            self.assertIsInstance(r, Result)
            self.assertEqual(set(r), {'found', 'value', 'reason'})
        self.assertEqual(set(results[2]), {'accepted', 'reason'})  # put 形状不变
        self.assertEqual(results[0].reason, 'missing')
        self.assertEqual(results[1].reason, 'missing')
        self.assertTrue(results[3].found)
        self.assertEqual(results[3].value, 1)

    def test_replay_get_with_reason_shape_matches_public_method(self):
        now = [0]
        live = EventCache(lambda: now[0])
        live.put('k', 'v', 100)
        live.put('g', 'gone', 1)
        now[0] = 10
        public_live = live.get_with_reason('k')
        public_expired = live.get_with_reason('g')
        public_missing = live.get_with_reason('g')

        replayed = EventCache(self.clock)
        results = replayed.replay_batch([
            (0, ('put', 'k', 'v', 100)),
            (0, ('put', 'g', 'gone', 1)),
            (10, ('get_with_reason', 'k')),
            (10, ('get_with_reason', 'g')),
            (11, ('get_with_reason', 'g')),
        ])
        self.assertEqual(dict(results[2]), dict(public_live))
        self.assertEqual(dict(results[3]), dict(public_expired))
        self.assertEqual(dict(results[4]), dict(public_missing))
        self.assertEqual(self.clock_calls[0], 0)

    def test_replay_apply_batch_rejects_get_with_reason(self):
        with self.assertRaises(ValueError):
            self.cache.apply_batch([('get_with_reason', 'k')])
        self.assertEqual(self.clock_calls[0], 0)
        self.assertEqual(self.cache.values, {})

    # ---- pop / pop_batch 记录：不读时间，过期事件原样返回 ----
    def test_replay_pop_returns_expired_events_without_time_judgement(self):
        results = self.cache.replay_batch([
            (10, ('push_expiring', 'd1', 'x', 10, 1)),   # 到期点 11
            (10, ('push', 'd2', 'e2', 10)),
            (100, ('pop',)),                              # 已到期也原样取出
            (100, ('pop',)),
            (100, ('pop',)),                              # 空队列 None
        ])
        self.assertEqual(results[2:], ['x', 'e2', None])

    def test_replay_pop_batch_limit_shapes(self):
        self.cache.replay_batch([
            (0, ('push', 'd1', 'a', 10)),
            (0, ('push', 'd2', 'b', 10)),
            (0, ('push', 'd3', 'c', 10)),
        ])
        results = self.cache.replay_batch([
            (1, ('pop_batch', 0)),     # 0：不取
            (1, ('pop_batch', 2)),     # 取队头两个
            (1, ('pop_batch', 10)),    # 超出长度：取剩余全部
            (1, ('pop_batch', None)),  # 空队列
            (1, ('pop_batch',)),       # 缺省形式等价 None
        ])
        self.assertEqual([type(r) for r in results], [list] * 5)
        self.assertEqual(results[0], [])
        self.assertEqual(results[1], ['a', 'b'])
        self.assertEqual(results[2], ['c'])
        self.assertEqual(results[3], [])
        self.assertEqual(results[4], [])

    def test_replay_pop_batch_returns_expired_events(self):
        self.cache.replay_batch([
            (0, ('push_expiring', 'd1', 'x', 10, 0)),
            (0, ('push_expiring', 'd2', 'y', 10, 1000)),
        ])
        results = self.cache.replay_batch([(100, ('pop_batch', None))])
        self.assertEqual(results[0], ['x', 'y'])  # 不做 TTL 判定

    # ---- peek 记录：纯观察，过期事件可见且状态不变 ----
    def test_replay_peek_is_non_destructive_and_ignores_ttl(self):
        self.cache.replay_batch([
            (0, ('push_expiring', 'd1', 'x', 10, 1)),
            (0, ('push', 'd2', 'a', 10)),
        ])
        results = self.cache.replay_batch([
            (100, ('peek', None)),
            (100, ('peek', 1)),
            (100, ('peek', 10)),
            (100, ('queue_status',)),
        ])
        self.assertEqual(results[0], ['x', 'a'])   # 过期项照样可见
        self.assertEqual(results[1], ['x'])
        self.assertEqual(results[2], ['x', 'a'])
        self.assertEqual(results[3].size, 2)       # 未移除任何项目
        self.assertEqual(list(self.cache.events), ['x', 'a'])

    # ---- pop_live_batch 记录：按记录时刻扫描、FIFO discarded、limit ----
    def test_replay_pop_live_batch_discards_in_fifo_order(self):
        results = self.cache.replay_batch([
            (0, ('push_expiring', 'd1', 'x1', 10, 1)),
            (0, ('push', 'd2', 'a', 10)),
            (0, ('push_expiring', 'd3', 'x2', 10, 1)),
            (0, ('push', 'd4', 'b', 10)),
            (0, ('push_expiring', 'd5', 'x3', 10, 1)),
            (10, ('pop_live_batch', None)),
        ])
        r = results[5]
        self.assertIsInstance(r, Result)
        self.assertEqual(r.events, ['a', 'b'])
        self.assertEqual([(d.event, d.reason) for d in r.discarded],
                         [('x1', 'event_ttl'), ('x2', 'event_ttl'), ('x3', 'event_ttl')])
        self.assertEqual(self.cache.queue_status().size, 0)

    def test_replay_pop_live_batch_boundary_equal_to_record_time(self):
        results = self.cache.replay_batch([
            (10, ('push_expiring', 'd', 'e', 10, 5)),   # 到期点 15
            (14, ('pop_live_batch', None)),              # 14 < 15：有效
        ])
        self.assertEqual(results[1].events, ['e'])
        self.assertEqual(results[1].discarded, [])
        # 重新排入一个同到期点事件，在恰为到期点的记录时刻判定
        results = self.cache.replay_batch([
            (14, ('push_expiring', 'd2', 'g', 10, 1)),  # 到期点 15
            (15, ('pop_live_batch', None)),
        ])
        self.assertEqual(results[1].events, [])
        self.assertEqual([d.event for d in results[1].discarded], ['g'])

    def test_replay_pop_live_batch_limit_stops_scan_keeps_tail(self):
        results = self.cache.replay_batch([
            (0, ('push_expiring', 'd1', 'x1', 10, 1)),  # 过期，被扫描丢弃
            (0, ('push_expiring', 'd2', 'a', 10, 500)),  # 有效 #1
            (0, ('push_expiring', 'd3', 'x2', 10, 1)),  # 过期，但不再扫描
            (0, ('push', 'd4', 'b', 10)),               # 不扫描
            (10, ('pop_live_batch', 1)),
            (10, ('queue_status',)),
        ])
        r = results[4]
        self.assertEqual(r.events, ['a'])
        self.assertEqual([d.event for d in r.discarded], ['x1'])
        self.assertEqual(list(self.cache.events), ['x2', 'b'])  # 未扫描尾部原样保留
        self.assertEqual(results[5].size, 2)

    def test_replay_pop_live_batch_zero_limit_changes_nothing(self):
        self.cache.replay_batch([
            (0, ('push_expiring', 'd', 'e', 10, 0)),
        ])
        results = self.cache.replay_batch([
            (100, ('pop_live_batch', 0)),
            (100, ('peek_live_batch', 0)),
            (100, ('queue_status',)),
        ])
        self.assertEqual((results[0].events, results[0].discarded), ([], []))
        self.assertEqual((results[1].events, results[1].discarded), ([], []))
        self.assertEqual(results[2].size, 1)       # 不扫描、不出队、不释放
        self.assertEqual(list(self.cache.event_expiries), [0])

    def test_replay_pop_live_batch_none_event_counts_as_live(self):
        results = self.cache.replay_batch([
            (0, ('push', 'd1', None, 10)),
            (10, ('pop_live_batch', 1)),
        ])
        self.assertEqual(results[1].events, [None])

    def test_replay_pop_live_batch_frees_slots_for_later_push(self):
        cache = self.make(1)
        results = cache.replay_batch([
            (0, ('push_expiring', 'd1', 'x', 100, 5)),
            (10, ('pop_live_batch', None)),          # 丢弃过期事件，释放唯一槽位
            (10, ('push', 'd2', 'new', 100)),       # 去重窗口仍在，但 d2 可用槽位
        ])
        self.assertEqual([d.event for d in results[1].discarded], ['x'])
        self.assertTrue(results[2].accepted)

    # ---- peek_live_batch 记录：只读，不过期清理、不释放槽位 ----
    def test_replay_peek_live_batch_reports_but_keeps_queue(self):
        cache = self.make(2)
        results = cache.replay_batch([
            (0, ('push_expiring', 'd1', 'x', 10, 1)),
            (0, ('push', 'd2', 'a', 10)),
            (10, ('peek_live_batch', None)),
            (10, ('queue_status',)),
            (10, ('push', 'd3', 'b', 10)),           # 槽位未释放 -> queue_full
        ])
        r = results[2]
        self.assertEqual(r.events, ['a'])
        self.assertEqual([d.event for d in r.discarded], ['x'])
        self.assertEqual(results[3].size, 2)
        self.assertEqual(results[4].reason, 'queue_full')
        self.assertEqual(list(cache.events), ['x', 'a'])  # 队列原样保留

    def test_replay_peek_then_pop_live_compose(self):
        results = self.cache.replay_batch([
            (0, ('push_expiring', 'd1', 'x', 10, 1)),
            (0, ('push', 'd2', 'a', 10)),
            (0, ('push_expiring', 'd3', 'y', 10, 1)),
            (10, ('peek_live_batch', 1)),    # 预览：discard x、live a，随后停止
            (10, ('pop_live_batch', 1)),     # 真正出队同样的 x 与 a
            (10, ('pop_batch', None)),       # 剩余 y（已过期）由不判时间路径取出
        ])
        self.assertEqual([d.event for d in results[3].discarded], ['x'])
        self.assertEqual(results[3].events, ['a'])
        self.assertEqual([d.event for d in results[4].discarded], ['x'])
        self.assertEqual(results[4].events, ['a'])
        self.assertEqual(results[5], ['y'])

    # ---- queue_status 记录 ----
    def test_replay_queue_status_reports_size_and_max(self):
        cache = self.make(2)
        results = cache.replay_batch([
            (0, ('push', 'd1', 'e1', 10)),
            (0, ('queue_status',)),
            (0, ('push', 'd2', 'e2', 10)),
            (1, ('queue_status',)),
            (1, ('pop',)),
            (1, ('queue_status',)),
        ])
        self.assertEqual((results[1].size, results[1].max_queue), (1, 2))
        self.assertEqual((results[3].size, results[3].max_queue), (2, 2))
        self.assertEqual((results[5].size, results[5].max_queue), (1, 2))

    # ---- 与公开入口的返回形状一致 ----
    def test_replay_read_shapes_match_public_operations(self):
        now = [0]
        live = EventCache(lambda: now[0], max_queue=3)
        live.put('k', 'v', 100)
        live.push_expiring('d1', 'x', 100, 1)
        live.push('d2', 'a', 100)
        now[0] = 10
        public = [
            live.get('k'),
            live.pop(),
            live.pop_batch(0),
            live.peek(None),
            live.pop_live_batch(1),
            live.peek_live_batch(None),
            live.queue_status(),
        ]

        replayed = EventCache(self.clock, max_queue=3)
        results = replayed.replay_batch([
            (0, ('put', 'k', 'v', 100)),
            (0, ('push_expiring', 'd1', 'x', 100, 1)),
            (0, ('push', 'd2', 'a', 100)),
            (10, ('get', 'k')),
            (10, ('pop',)),
            (10, ('pop_batch', 0)),
            (10, ('peek', None)),
            (10, ('pop_live_batch', 1)),
            (10, ('peek_live_batch', None)),
            (10, ('queue_status',)),
        ])
        got = results[3:]
        self.assertEqual(got[0], public[0])
        self.assertEqual(got[1], public[1])
        self.assertEqual(got[2], public[2])
        self.assertEqual(got[3], public[3])
        self.assertEqual(dict(got[4]), dict(public[4]))
        self.assertEqual(dict(got[5]), dict(public[5]))
        self.assertEqual(dict(got[6]), dict(public[6]))

    # ---- 读取记录的校验：结构、limit、键 ----
    def test_invalid_read_record_raises_value_error_atomically(self):
        self.cache.put('k', 'v', 100)
        self.cache.push('d0', 'e0', 100)
        before = (dict(self.cache.values), list(self.cache.events),
                  list(self.cache.event_expiries), dict(self.cache.seen))
        for bad in (
            [(1, ('get',))],                       # get 缺 key
            [(1, ('get', 'a', 'extra'))],
            [(1, ('get_with_reason',))],           # get_with_reason 缺 key
            [(1, ('get_with_reason', 'a', 'extra'))],
            [(1, ('pop', 'x'))],                   # pop 不接受参数
            [(1, ('pop_batch', -1))],
            [(1, ('pop_batch', 1.5))],
            [(1, ('pop_batch', True))],
            [(1, ('peek', '3'))],
            [(1, ('pop_live_batch', 2.0))],
            [(1, ('peek_live_batch', [1]))],
            [(1, ('queue_status', 'x'))],
            [(1, ('pop_batch', 1, 2))],
            [(1, ('unknown_read',))],
            [(1, ('pop_batch', 1)), (0, ('pop',))],   # 时间倒退
        ):
            calls_before = self.clock_calls[0]
            with self.assertRaises(ValueError):
                self.cache.replay_batch(bad)
            self.assertEqual(self.clock_calls[0], calls_before)
            after = (dict(self.cache.values), list(self.cache.events),
                     list(self.cache.event_expiries), dict(self.cache.seen))
            self.assertEqual(before, after)

    def test_unhashable_get_key_raises_type_error_atomically(self):
        self.cache.put('k', 'v', 100)
        before = (dict(self.cache.values), list(self.cache.events), dict(self.cache.seen))
        for op in (('get', ['unhashable']), ('get_with_reason', ['unhashable'])):
            with self.assertRaises(TypeError):
                self.cache.replay_batch([(1, op)])
            after = (dict(self.cache.values), list(self.cache.events), dict(self.cache.seen))
            self.assertEqual(before, after)

    def test_read_record_generator_failing_mid_validation_changes_nothing(self):
        self.cache.replay_batch([(0, ('push', 'd', 'e', 10))])

        def gen():
            yield (1, ('pop_batch', 1))
            yield (2, ('peek_live_batch', -1))  # 非法 limit

        with self.assertRaises(ValueError):
            self.cache.replay_batch(gen())
        self.assertEqual(list(self.cache.events), ['e'])  # 前段记录也不得生效


class DiscardHistoryTest(unittest.TestCase):
    def setUp(self):
        self.now = [100]
        self.clock_calls = [0]

        def clock():
            self.clock_calls[0] += 1
            return self.now[0]

        self.clock = clock

    def make(self, max_queue=None, overflow_policy='reject_new', history_limit=None):
        return EventCache(self.clock, max_queue=max_queue,
                          overflow_policy=overflow_policy,
                          discard_history_limit=history_limit)

    def advance(self, seconds):
        self.now[0] += seconds

    def triple(self, entry):
        return (entry.event, entry.reason, entry.timestamp)

    # ---- 构造参数校验 ----
    def test_default_history_limit_is_unlimited(self):
        cache = self.make()
        self.assertIsNone(cache.discard_history_limit)
        self.assertEqual(cache.discard_history(), [])

    def test_invalid_history_limit_raises_value_error(self):
        for bad in (-1, -10, True, False, 1.5, 2.0, '3', [], object()):
            with self.assertRaises(ValueError):
                self.make(history_limit=bad)

    def test_invalid_history_limit_does_not_read_clock(self):
        calls = [0]

        def counting_clock():
            calls[0] += 1
            return 0

        for bad in (-1, True, 'x'):
            with self.assertRaises(ValueError):
                EventCache(counting_clock, discard_history_limit=bad)
        self.assertEqual(calls[0], 0)

    def test_zero_and_positive_and_none_limits_accepted(self):
        for limit in (None, 0, 1, 10 ** 6):
            cache = self.make(history_limit=limit)
            self.assertIs(cache.discard_history_limit, limit)

    # ---- event_ttl 清理写入历史 ----
    def test_cleanup_expired_events_records_history(self):
        cache = self.make()
        cache.push_expiring('d1', 'e1', 100, 5)   # 到期点 105
        cache.push('d2', 'e2', 100)
        cache.push_expiring('d3', 'e3', 100, 500)
        self.advance(10)  # 110
        result = cache.cleanup_expired_events()
        self.assertEqual(result.events_removed, 1)
        history = cache.discard_history()
        self.assertEqual(len(history), 1)
        self.assertEqual(self.triple(history[0]), ('e1', 'event_ttl', 110))

    def test_discard_expired_events_records_history_with_observation_time(self):
        cache = self.make()
        cache.push_expiring('d', 'e', 100, 5)
        self.advance(7)  # 107
        r = cache.discard_expired_events()
        # 返回的 discarded 条目保持既有形状（无 timestamp），且与历史条目是不同对象
        self.assertEqual(set(r.discarded[0]), {'event', 'reason'})
        history = cache.discard_history()
        self.assertEqual(set(history[0]), {'event', 'reason', 'timestamp'})
        self.assertEqual(self.triple(history[0]), ('e', 'event_ttl', 107))
        self.assertIsNot(r.discarded[0], history[0])

    def test_cleanup_all_expired_records_event_history(self):
        cache = self.make()
        cache.put('k', 'v', 5)
        cache.push_expiring('d', 'e', 100, 5)
        self.advance(10)  # 110
        cache.cleanup_all_expired()
        history = cache.discard_history()
        # values/seen 的清理不写历史，只有事件一条
        self.assertEqual([self.triple(h) for h in history], [('e', 'event_ttl', 110)])

    def test_multiple_ttl_events_recorded_in_fifo_order_with_same_timestamp(self):
        cache = self.make()
        cache.push_expiring('d1', 'x1', 100, 1)
        cache.push_expiring('d2', 'a', 100, 500)
        cache.push_expiring('d3', 'x2', 100, 1)
        cache.push_expiring('d4', 'x3', 100, 2)
        self.advance(10)  # 110
        cache.cleanup_expired_events()
        self.assertEqual([self.triple(h) for h in cache.discard_history()], [
            ('x1', 'event_ttl', 110),
            ('x2', 'event_ttl', 110),
            ('x3', 'event_ttl', 110),
        ])

    def test_pop_live_batch_records_expired_in_scan_order(self):
        cache = self.make()
        cache.push_expiring('d1', 'x1', 100, 1)
        cache.push('d2', 'a', 100)
        cache.push_expiring('d3', 'x2', 100, 1)
        self.advance(10)  # 110
        r = cache.pop_live_batch()
        # 返回的 discarded 仍是两字段形状
        self.assertEqual([set(d) for d in r.discarded], [{'event', 'reason'}] * 2)
        self.assertEqual([self.triple(h) for h in cache.discard_history()], [
            ('x1', 'event_ttl', 110),
            ('x2', 'event_ttl', 110),
        ])

    def test_pop_live_batch_timestamp_matches_its_clock_read(self):
        # 时间戳是触发本次出队的观察时刻，而非事件到期点
        cache = self.make()
        cache.push_expiring('d', 'e', 100, 5)  # 到期点 105
        self.advance(5)  # 恰为 105：<= 边界
        cache.pop_live_batch()
        self.assertEqual(cache.discard_history()[0].timestamp, 105)

        # 独立时钟：到期点同为 105，但观察时刻拖到 300，时间戳取 300
        other_now = [100]
        cache2 = EventCache(lambda: other_now[0])
        cache2.push_expiring('d2', 'e2', 100, 5)
        other_now[0] = 300
        cache2.pop_live_batch()
        self.assertEqual(cache2.discard_history()[0].timestamp, 300)

    # ---- drop_oldest 挤出写入历史 ----
    def test_drop_oldest_eviction_records_queue_full_history(self):
        cache = self.make(max_queue=2, overflow_policy='drop_oldest')
        cache.push('d1', 'e1', 100)
        cache.push('d2', 'e2', 100)
        self.advance(5)  # 105
        r = cache.push_with_reason('d3', 'e3', 100)
        self.assertTrue(r.accepted)
        self.assertEqual(r.discarded[0].event, 'e1')  # 既有 discarded 形状不变
        self.assertEqual(set(r.discarded[0]), {'event', 'reason'})
        history = cache.discard_history()
        self.assertEqual(self.triple(history[0]), ('e1', 'queue_full', 105))
        self.assertEqual([cache.pop(), cache.pop()], ['e2', 'e3'])

    def test_push_batch_evictions_use_batch_moment_in_order(self):
        cache = self.make(max_queue=2, overflow_policy='drop_oldest')
        # 整批同一时钟时刻 100：第 3 项挤出 e1，第 4 项挤出 e2
        results = cache.push_batch([
            ('d1', 'e1', 100), ('d2', 'e2', 100),
            ('d3', 'e3', 100), ('d4', 'e4', 100),
        ])
        self.assertTrue(all(r.accepted for r in results))
        self.assertEqual([self.triple(h) for h in cache.discard_history()], [
            ('e1', 'queue_full', 100),
            ('e2', 'queue_full', 100),
        ])
        self.assertEqual(cache.pop_batch(), ['e3', 'e4'])

    def test_apply_batch_ttl_and_eviction_history_follow_operation_order(self):
        cache = self.make(max_queue=1, overflow_policy='drop_oldest')
        results = cache.apply_batch([
            ('push_expiring', 'd1', 'old', 100, 0),  # 接受时已到期，占槽
            ('cleanup_expired_events',),              # 清掉 old -> event_ttl
            ('push', 'd2', 'a', 100),                # 入队
            ('push', 'd3', 'b', 100),                # 挤出 a -> queue_full
        ])
        self.assertEqual(results[1].events_removed, 1)
        self.assertTrue(results[3].accepted)
        self.assertEqual([self.triple(h) for h in cache.discard_history()], [
            ('old', 'event_ttl', 100),
            ('a', 'queue_full', 100),
        ])

    # ---- 不写历史的路径 ----
    def test_plain_pop_and_pop_batch_do_not_record_history(self):
        cache = self.make()
        cache.push_expiring('d1', 'x', 100, 0)  # 已到期
        cache.push('d2', 'y', 100)
        self.advance(100)
        self.assertEqual(cache.pop(), 'x')
        self.assertEqual(cache.pop_batch(), ['y'])
        self.assertEqual(cache.discard_history(), [])

    def test_reject_new_queue_full_does_not_record_history(self):
        cache = self.make(max_queue=1)
        cache.push('d1', 'e1', 100)
        r = cache.push_with_reason('d2', 'e2', 100)
        self.assertEqual(r.reason, 'queue_full')
        self.assertEqual(cache.discard_history(), [])

    def test_dedupe_rejection_does_not_record_history(self):
        cache = self.make()
        cache.push('d', 'e1', 100)
        self.assertEqual(cache.push_with_reason('d', 'e2', 100).reason, 'dedupe_window')
        self.assertEqual(cache.discard_history(), [])

    def test_peek_live_batch_never_records_history(self):
        cache = self.make()
        cache.push_expiring('d', 'x', 100, 0)
        self.advance(10)
        r = cache.peek_live_batch()
        self.assertEqual([d.event for d in r.discarded], ['x'])
        self.assertEqual(cache.discard_history(), [])  # 纯观察不留历史
        self.assertEqual(cache.queue_status().size, 1)

    def test_get_expiry_and_value_cleanup_do_not_record_history(self):
        cache = self.make()
        cache.put('k', 'v', 0)
        cache.push_expiring('d', 'e', 100, 100)  # 事件未到期
        self.advance(1)
        self.assertIsNone(cache.get('k'))         # 值过期不写历史
        self.assertEqual(cache.cleanup().values_removed, 0)
        cache.put('k2', 'v2', 0)
        self.assertEqual(cache.cleanup().values_removed, 1)  # 批量值清理不写
        self.assertEqual(cache.discard_history(), [])

    # ---- None 事件也记录 ----
    def test_none_event_is_recorded(self):
        cache = self.make(max_queue=1, overflow_policy='drop_oldest')
        cache.push('d1', None, 100)
        cache.push('d2', 'second', 100)
        history = cache.discard_history()
        self.assertEqual(len(history), 1)
        self.assertIsNone(history[0].event)
        self.assertEqual((history[0].reason, history[0].timestamp), ('queue_full', 100))

    def test_none_event_recorded_on_ttl_cleanup(self):
        cache = self.make()
        cache.push_expiring('d', None, 100, 0)
        cache.cleanup_expired_events()
        history = cache.discard_history()
        self.assertEqual(len(history), 1)
        self.assertIsNone(history[0].event)
        self.assertEqual(history[0].reason, 'event_ttl')

    # ---- 容量上限：只保留最近记录 ----
    def test_finite_limit_keeps_most_recent(self):
        cache = self.make(history_limit=2)
        for i in range(5):
            cache.push_expiring('d%d' % i, 'e%d' % i, 100, 0)
            cache.cleanup_expired_events()  # 每步 t=100+i（push 与 cleanup 各读一次钟但同值）
        history = cache.discard_history()
        self.assertEqual(len(history), 2)
        self.assertEqual([h.event for h in history], ['e3', 'e4'])

    def test_zero_limit_retains_nothing_but_operations_still_succeed(self):
        cache = self.make(history_limit=0)
        cache.push_expiring('d', 'e', 100, 0)
        self.assertEqual(cache.cleanup_expired_events().events_removed, 1)
        self.assertEqual(cache.discard_history(), [])
        # 丢弃仍返回即时结果
        r = cache.discard_expired_events()
        self.assertEqual(r.events_removed, 0)

    def test_zero_limit_with_drop_oldest_retains_nothing(self):
        cache = self.make(max_queue=1, overflow_policy='drop_oldest', history_limit=0)
        cache.push('d1', 'e1', 100)
        cache.push('d2', 'e2', 100)  # 挤出 e1 但不留历史
        self.assertEqual(cache.discard_history(), [])
        self.assertEqual(cache.pop(), 'e2')

    def test_unlimited_history_grows_unbounded(self):
        cache = self.make()
        for i in range(50):
            cache.push_expiring('d%d' % i, i, 100, 0)
            cache.cleanup_expired_events()
        self.assertEqual(len(cache.discard_history()), 50)

    # ---- discard_history(limit) 读取 ----
    def test_history_returns_independent_detached_results(self):
        cache = self.make()
        cache.push_expiring('d', 'e', 100, 0)
        cache.cleanup_expired_events()
        first = cache.discard_history()
        second = cache.discard_history()
        self.assertEqual(first, second)
        self.assertIsNot(first, second)
        self.assertIsNot(first[0], second[0])
        first[0]['event'] = 'hacked'
        first.append(Result(event='x', reason='event_ttl', timestamp=1))
        internal = cache.discard_history()
        self.assertEqual(internal[0].event, 'e')
        self.assertEqual(len(internal), 1)

    def test_history_limit_returns_recent_entries_only(self):
        cache = self.make()
        for i, ev in enumerate(['a', 'b', 'c', 'd']):
            cache.push_expiring('d%d' % i, ev, 100, 0)
            cache.cleanup_expired_events()
        self.assertEqual([h.event for h in cache.discard_history(2)], ['c', 'd'])
        self.assertEqual([h.event for h in cache.discard_history(0)], [])
        self.assertEqual([h.event for h in cache.discard_history(10)], ['a', 'b', 'c', 'd'])
        self.assertEqual([h.event for h in cache.discard_history(None)], ['a', 'b', 'c', 'd'])
        # 带 limit 的读取不改变内部历史
        self.assertEqual(len(cache.discard_history()), 4)

    def test_history_limit_validation_does_not_read_clock_or_mutate(self):
        cache = self.make()
        cache.push_expiring('d', 'e', 100, 0)
        cache.cleanup_expired_events()
        self.clock_calls[0] = 0
        for bad in (-1, -5, 1.5, 2.0, '2', [2], True, False, object()):
            with self.assertRaises(ValueError):
                cache.discard_history(bad)
        self.assertEqual(self.clock_calls[0], 0)
        self.assertEqual(len(cache.discard_history()), 1)

    def test_history_zero_limit_does_not_read_clock(self):
        cache = self.make()
        cache.push_expiring('d', 'e', 100, 0)
        self.clock_calls[0] = 0
        self.assertEqual(cache.discard_history(0), [])
        self.assertEqual(self.clock_calls[0], 0)

    def test_history_entries_are_results_with_attribute_access(self):
        cache = self.make()
        cache.push_expiring('d', 'e', 100, 0)
        cache.cleanup_expired_events()
        entry = cache.discard_history()[0]
        self.assertIsInstance(entry, Result)
        self.assertEqual(entry.event, 'e')
        self.assertEqual(entry.reason, 'event_ttl')
        self.assertIsInstance(entry.timestamp, (int, float))
        self.assertEqual(entry['timestamp'], entry.timestamp)

    # ---- clear_discard_history ----
    def test_clear_returns_count_and_empties_history(self):
        cache = self.make()
        for i in range(3):
            cache.push_expiring('d%d' % i, i, 100, 0)
            cache.cleanup_expired_events()
        self.assertEqual(len(cache.discard_history()), 3)
        self.assertEqual(cache.clear_discard_history(), 3)
        self.assertEqual(cache.discard_history(), [])
        self.assertEqual(cache.clear_discard_history(), 0)  # 再次清空返回 0

    def test_clear_does_not_read_clock_or_touch_other_state(self):
        cache = self.make(max_queue=2, overflow_policy='drop_oldest')
        cache.put('k', 'v', 100)
        cache.push('d1', 'e1', 100)
        cache.push('d2', 'e2', 100)
        cache.push('d3', 'e3', 100)  # 挤出 e1
        events_before = list(cache.events)
        self.clock_calls[0] = 0
        removed = cache.clear_discard_history()
        self.assertEqual(removed, 1)
        self.assertEqual(self.clock_calls[0], 0)
        self.assertEqual(list(cache.events), events_before)
        self.assertEqual(cache.get('k'), 'v')
        self.assertEqual(cache.queue_status().size, 2)

    def test_clear_then_history_accumulates_again(self):
        cache = self.make()
        cache.push_expiring('d', 'e1', 100, 0)
        cache.cleanup_expired_events()
        cache.clear_discard_history()
        cache.push_expiring('d2', 'e2', 100, 0)
        cache.cleanup_expired_events()
        self.assertEqual([h.event for h in cache.discard_history()], ['e2'])


class DiscardHistorySnapshotTest(unittest.TestCase):
    def setUp(self):
        self.now = [100]
        self.clock_calls = [0]

        def clock():
            self.clock_calls[0] += 1
            return self.now[0]

        self.clock = clock

    def make(self, **kwargs):
        return EventCache(self.clock, **kwargs)

    def seed_history(self, cache):
        cache.push_expiring('d1', 'e1', 100, 5)   # 105
        cache.push_expiring('d2', 'e2', 100, 500)
        self.now[0] = 110
        cache.cleanup_expired_events()             # e1 -> (e1, event_ttl, 110)

    # ---- 快照字段出现条件 ----
    def test_default_snapshot_keeps_core_and_receipt_fields(self):
        cache = self.make()
        cache.push('d', 'e', 100)
        self.assertEqual(set(cache.snapshot()), {
            'values', 'events', 'seen', 'max_queue',
            'event_receipts', 'next_receipt', 'event_metadata'})

    def test_snapshot_includes_history_when_nonempty(self):
        cache = self.make()
        self.seed_history(cache)
        snap = cache.snapshot()
        self.assertIn('discard_history', snap)
        self.assertIn('discard_history_limit', snap)
        self.assertIsNone(snap.discard_history_limit)
        entries = snap.discard_history
        self.assertEqual(len(entries), 1)
        self.assertEqual((entries[0].event, entries[0].reason, entries[0].timestamp),
                         ('e1', 'event_ttl', 110))

    def test_snapshot_includes_limit_when_nondefault_even_if_empty(self):
        for limit in (0, 5):
            cache = self.make(discard_history_limit=limit)
            snap = cache.snapshot()
            self.assertEqual(snap.discard_history, [])
            self.assertEqual(snap['discard_history_limit'], limit)

    def test_fields_disappear_after_clear_with_default_limit(self):
        cache = self.make()
        self.seed_history(cache)
        cache.clear_discard_history()
        snap = cache.snapshot()
        self.assertNotIn('discard_history', snap)
        self.assertNotIn('discard_history_limit', snap)

    def test_snapshot_history_detached_from_cache(self):
        cache = self.make()
        self.seed_history(cache)
        snap = cache.snapshot()
        snap['discard_history'].append(Result(event='x', reason='event_ttl', timestamp=1))
        snap['discard_history'][0]['event'] = 'hacked'
        internal = cache.discard_history()
        self.assertEqual(len(internal), 1)
        self.assertEqual(internal[0].event, 'e1')

    # ---- restore：旧快照兼容 ----
    def test_restore_old_snapshot_gives_empty_unlimited_history(self):
        cache = self.make()
        self.seed_history(cache)
        cache.restore({'values': {}, 'events': [], 'seen': {}, 'max_queue': None})
        self.assertEqual(cache.discard_history(), [])
        self.assertIsNone(cache.discard_history_limit)
        # 旧快照：空对齐回执列表、计数器回到 1；快照始终含回执两字段
        self.assertEqual(list(cache.event_receipts), [])
        self.assertEqual(cache.snapshot().next_receipt, 1)
        self.assertEqual(set(cache.snapshot()), {
            'values', 'events', 'seen', 'max_queue',
            'event_receipts', 'next_receipt', 'event_metadata'})

    def test_restore_history_only_field_implies_unlimited_limit(self):
        cache = self.make()
        cache.restore({
            'values': {}, 'events': [], 'seen': {}, 'max_queue': None,
            'discard_history': [{'event': 'e', 'reason': 'queue_full', 'timestamp': 7}],
        })
        self.assertIsNone(cache.discard_history_limit)
        entry = cache.discard_history()[0]
        self.assertEqual((entry.event, entry.reason, entry.timestamp), ('e', 'queue_full', 7))
        self.assertIsInstance(entry, Result)

    def test_restore_limit_only_field_implies_empty_history(self):
        cache = self.make()
        self.seed_history(cache)
        cache.restore({
            'values': {}, 'events': [], 'seen': {}, 'max_queue': None,
            'discard_history_limit': 3,
        })
        self.assertEqual(cache.discard_history(), [])
        self.assertEqual(cache.discard_history_limit, 3)

    def test_restore_deep_copies_history_containers(self):
        source = {'event': 'e', 'reason': 'event_ttl', 'timestamp': 5}
        snap = {
            'values': {}, 'events': [], 'seen': {}, 'max_queue': None,
            'discard_history': [source], 'discard_history_limit': None,
        }
        cache = self.make()
        cache.restore(snap)
        snap['discard_history'].append({'event': 'z', 'reason': 'queue_full', 'timestamp': 6})
        source['event'] = 'mutated'
        history = cache.discard_history()
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0].event, 'e')  # 源映射的后续变更不影响恢复结果

    def test_round_trip_preserves_history(self):
        cache = self.make(max_queue=1, overflow_policy='drop_oldest')
        cache.push('d1', 'e1', 100)
        self.now[0] = 105
        cache.push('d2', 'e2', 100)   # 挤出 e1 -> queue_full @105
        self.assertEqual(cache.pop(), 'e2')  # 消费不写历史，腾出槽位
        cache.push_expiring('d3', 'x', 100, 0)  # 接受点 105 即到期
        self.now[0] = 120
        cache.cleanup_expired_events()  # x -> event_ttl @120
        snap1 = cache.snapshot()
        rebuilt = self.make(discard_history_limit=99)
        rebuilt.restore(snap1)
        self.assertEqual(rebuilt.snapshot(), snap1)
        self.assertEqual([(h.event, h.reason, h.timestamp) for h in rebuilt.discard_history()],
                         [('e1', 'queue_full', 105), ('x', 'event_ttl', 120)])

    def test_round_trip_with_finite_limit(self):
        cache = self.make(discard_history_limit=2)
        for i in range(4):
            cache.push_expiring('d%d' % i, i, 100, 0)
            self.now[0] += 1
            cache.cleanup_expired_events()
        self.assertEqual([h.event for h in cache.discard_history()], [2, 3])
        rebuilt = self.make()
        rebuilt.restore(cache.snapshot())
        self.assertEqual(rebuilt.discard_history_limit, 2)
        self.assertEqual([h.event for h in rebuilt.discard_history()], [2, 3])

    def test_restore_trims_history_exceeding_manual_snapshot_limit(self):
        # 手工构造的快照：容量 2 却给了 3 条 -> 只恢复最近 2 条
        cache = self.make()
        cache.restore({
            'values': {}, 'events': [], 'seen': {}, 'max_queue': None,
            'discard_history': [
                {'event': 'a', 'reason': 'event_ttl', 'timestamp': 1},
                {'event': 'b', 'reason': 'queue_full', 'timestamp': 2},
                {'event': 'c', 'reason': 'event_ttl', 'timestamp': 3},
            ],
            'discard_history_limit': 2,
        })
        self.assertEqual([h.event for h in cache.discard_history()], ['b', 'c'])

    def test_restore_zero_limit_with_entries_gives_empty_history(self):
        cache = self.make()
        cache.restore({
            'values': {}, 'events': [], 'seen': {}, 'max_queue': None,
            'discard_history': [{'event': 'a', 'reason': 'event_ttl', 'timestamp': 1}],
            'discard_history_limit': 0,
        })
        self.assertEqual(cache.discard_history(), [])
        self.assertEqual(cache.discard_history_limit, 0)

    # ---- restore 校验失败：原子、不读时钟 ----
    def assert_rejected_preserving_history(self, bad):
        # 每次断言重建种子状态：时钟重置回 100，保证 seed_history 内 e1 的
        # 到期点 105 确实早于清理时刻 110（共享时钟会在上一轮停在 110）
        self.now[0] = 100
        cache = self.make()
        self.seed_history(cache)
        before = [self.triple(h) for h in cache.discard_history()]
        calls_before = self.clock_calls[0]
        with self.assertRaises(ValueError):
            cache.restore(bad)
        self.assertEqual(self.clock_calls[0], calls_before)
        self.assertEqual([self.triple(h) for h in cache.discard_history()], before)
        self.assertEqual(cache.queue_status().size, 1)  # e2 仍在队

    def triple(self, entry):
        return (entry.event, entry.reason, entry.timestamp)

    def base(self, **extra):
        snap = {'values': {}, 'events': [], 'seen': {}, 'max_queue': None}
        snap.update(extra)
        return snap

    def test_restore_rejects_bad_history_container(self):
        for raw in ((), {}, 'x', 42):
            self.assert_rejected_preserving_history(self.base(discard_history=raw))

    def test_restore_rejects_bad_history_entry_structure(self):
        for entry in (
            'not-a-mapping',
            ('e', 'event_ttl', 1),                    # 非映射
            {'event': 'e', 'reason': 'event_ttl'},    # 缺 timestamp
            {'event': 'e', 'timestamp': 1},           # 缺 reason
            {'reason': 'event_ttl', 'timestamp': 1},  # 缺 event
            {'event': 'e', 'reason': 'event_ttl', 'timestamp': 1, 'x': 2},  # 多余字段
        ):
            self.assert_rejected_preserving_history(self.base(discard_history=[entry]))

    def test_restore_rejects_bad_reason(self):
        for reason in ('expired', 'ttl', 'dedupe_window', None, 1, True):
            self.assert_rejected_preserving_history(self.base(discard_history=[
                {'event': 'e', 'reason': reason, 'timestamp': 1}]))

    def test_restore_rejects_bad_timestamp(self):
        for ts in (True, False, None, '1', float('nan'), float('inf'),
                   -float('inf'), 1.0j, [1]):
            self.assert_rejected_preserving_history(self.base(discard_history=[
                {'event': 'e', 'reason': 'event_ttl', 'timestamp': ts}]))

    def test_restore_accepts_negative_and_float_timestamps(self):
        cache = self.make()
        cache.restore(self.base(discard_history=[
            {'event': 'a', 'reason': 'event_ttl', 'timestamp': -5},
            {'event': 'b', 'reason': 'queue_full', 'timestamp': 2.5},
        ]))
        self.assertEqual([h.timestamp for h in cache.discard_history()], [-5, 2.5])

    def test_restore_rejects_bad_history_limit(self):
        for limit in (-1, True, False, 1.5, 2.0, '3', []):
            self.assert_rejected_preserving_history(
                self.base(discard_history=[], discard_history_limit=limit))

    def test_restore_rejects_unknown_field_alongside_history(self):
        self.assert_rejected_preserving_history(
            self.base(discard_history=[], bogus=1))

    def test_failed_restore_keeps_history_functional(self):
        cache = self.make()
        self.seed_history(cache)
        with self.assertRaises(ValueError):
            cache.restore(self.base(discard_history='nope'))
        # 历史与队列仍可继续使用并继续累积
        cache.clear_discard_history()
        cache.push_expiring('d9', 'new', 100, 0)
        cache.cleanup_expired_events()
        self.assertEqual([h.event for h in cache.discard_history()], ['new'])


class DiscardHistoryReplayTest(unittest.TestCase):
    def setUp(self):
        self.now = [100]
        self.clock_calls = [0]

        def clock():
            self.clock_calls[0] += 1
            return self.now[0]

        self.clock = clock

    def make(self, **kwargs):
        return EventCache(self.clock, **kwargs)

    def triple(self, entry):
        return (entry.event, entry.reason, entry.timestamp)

    def test_replay_ttl_cleanup_and_eviction_use_record_timestamps(self):
        cache = self.make(max_queue=1, overflow_policy='drop_oldest')
        results = cache.replay_batch([
            (10, ('push_expiring', 'd1', 'old', 100, 0)),  # 10 接受即到期
            (20, ('cleanup_expired_events',)),              # old -> event_ttl @20
            (20, ('push', 'd2', 'a', 100)),
            (30, ('push', 'd3', 'b', 100)),                 # 挤出 a -> queue_full @30
            (40, ('discard_expired_events',)),              # 无到期事件
        ])
        self.assertEqual(results[1].events_removed, 1)
        self.assertTrue(results[3].accepted)
        self.assertEqual(self.clock_calls[0], 0)  # 全程不读注入时钟
        self.assertEqual([self.triple(h) for h in cache.discard_history()], [
            ('old', 'event_ttl', 20),
            ('a', 'queue_full', 30),
        ])

    def test_replay_discard_expired_events_records_history(self):
        cache = self.make()
        cache.replay_batch([
            (0, ('push_expiring', 'd1', 'x1', 10, 1)),
            (0, ('push', 'd2', 'a', 10)),
            (0, ('push_expiring', 'd3', 'x2', 10, 1)),
        ])
        r = cache.replay_batch([(5, ('discard_expired_events',))])
        self.assertEqual([d.event for d in r[0].discarded], ['x1', 'x2'])
        self.assertEqual([self.triple(h) for h in cache.discard_history()], [
            ('x1', 'event_ttl', 5),
            ('x2', 'event_ttl', 5),
        ])
        self.assertEqual(self.clock_calls[0], 0)

    def test_replay_pop_live_batch_records_history_but_pop_does_not(self):
        cache = self.make()
        cache.replay_batch([
            (0, ('push_expiring', 'd1', 'x', 10, 1)),
            (0, ('push', 'd2', 'a', 10)),
            (5, ('pop_live_batch', None)),   # x 过期 -> 历史 @5
        ])
        self.assertEqual([self.triple(h) for h in cache.discard_history()],
                         [('x', 'event_ttl', 5)])
        # 普通 pop / pop_batch 即使取出已到期事件也不写历史
        cache2 = self.make()
        cache2.replay_batch([
            (0, ('push_expiring', 'd1', 'x', 10, 0)),
            (0, ('push', 'd2', 'a', 10)),
            (9, ('pop',)),
            (9, ('pop_batch', None)),
        ])
        self.assertEqual(cache2.discard_history(), [])
        self.assertEqual(self.clock_calls[0], 0)

    def test_replay_rejections_do_not_record_history(self):
        cache = self.make(max_queue=1)  # reject_new
        cache.replay_batch([
            (0, ('push', 'd1', 'e1', 100)),
            (1, ('push', 'd2', 'e2', 100)),   # queue_full 拒绝
            (2, ('push', 'd1', 'dup', 100)),  # dedupe_window 拒绝
        ])
        self.assertEqual(cache.discard_history(), [])
        self.assertEqual(self.clock_calls[0], 0)

    def test_replay_peek_live_batch_does_not_record_history(self):
        cache = self.make()
        cache.replay_batch([
            (0, ('push_expiring', 'd', 'x', 10, 0)),
            (5, ('peek_live_batch', None)),
        ])
        self.assertEqual(cache.discard_history(), [])
        self.assertEqual(cache.queue_status().size, 1)

    def test_replay_cleanup_all_expired_records_event_history(self):
        cache = self.make()
        cache.replay_batch([
            (0, ('put', 'k', 'v', 5)),
            (0, ('push_expiring', 'd', 'e', 10, 5)),
            (10, ('cleanup_all_expired',)),
        ])
        self.assertEqual([self.triple(h) for h in cache.discard_history()],
                         [('e', 'event_ttl', 10)])

    def test_replay_history_matches_live_calls(self):
        # 同样的操作序列：一路实时调用、一路逻辑时间回放，历史应逐项一致
        def build_live():
            c = EventCache(lambda: live_now[0], max_queue=1, overflow_policy='drop_oldest')
            live_now[0] = 10
            c.push_expiring('d1', 'old', 100, 0)
            live_now[0] = 20
            c.cleanup_expired_events()
            c.push('d2', 'a', 100)
            live_now[0] = 30
            c.push('d3', 'b', 100)
            return c

        live_now = [0]
        live = build_live()
        replayed = self.make(max_queue=1, overflow_policy='drop_oldest')
        replayed.replay_batch([
            (10, ('push_expiring', 'd1', 'old', 100, 0)),
            (20, ('cleanup_expired_events',)),
            (20, ('push', 'd2', 'a', 100)),
            (30, ('push', 'd3', 'b', 100)),
        ])
        self.assertEqual(self.clock_calls[0], 0)
        live_hist = [(h.event, h.reason, h.timestamp) for h in live.discard_history()]
        replay_hist = [(h.event, h.reason, h.timestamp) for h in replayed.discard_history()]
        self.assertEqual(live_hist, replay_hist)
        self.assertEqual(live_hist, [('old', 'event_ttl', 20), ('a', 'queue_full', 30)])

    def test_replay_history_survives_snapshot_restore_round_trip(self):
        cache = self.make()
        cache.replay_batch([
            (10, ('push_expiring', 'd', 'e', 10, 5)),
            (20, ('cleanup_expired_events',)),
        ])
        snap = cache.snapshot()
        rebuilt = self.make(discard_history_limit=99)
        rebuilt.replay_batch([])  # 确保不读时钟也无副作用
        rebuilt.restore(snap)
        self.assertEqual([self.triple(h) for h in rebuilt.discard_history()],
                         [('e', 'event_ttl', 20)])


class ReceiptTest(unittest.TestCase):
    def setUp(self):
        self.now = [100]
        self.clock_calls = [0]

        def clock():
            self.clock_calls[0] += 1
            return self.now[0]

        self.clock = clock
        self.cache = EventCache(clock)

    def advance(self, seconds):
        self.now[0] += seconds

    def make(self, max_queue=None, overflow_policy='reject_new'):
        return EventCache(self.clock, max_queue=max_queue,
                          overflow_policy=overflow_policy)

    # ---- 回执分配：从 1 起递增、不复用 ----
    def test_push_with_receipt_allocates_from_one(self):
        r1 = self.cache.push_with_receipt('d1', 'e1', 10)
        r2 = self.cache.push_with_receipt('d2', 'e2', 10)
        self.assertEqual(dict(r1), {'receipt': 1, 'accepted': True, 'reason': None})
        self.assertEqual(dict(r2), {'receipt': 2, 'accepted': True, 'reason': None})
        self.assertIsInstance(r1, Result)

    def test_push_expiring_with_receipt_allocates_same_sequence(self):
        r1 = self.cache.push_with_receipt('d1', 'e1', 10)
        r2 = self.cache.push_expiring_with_receipt('d2', 'e2', 10, 5)
        self.assertEqual((r1.receipt, r2.receipt), (1, 2))
        self.assertEqual(list(self.cache.event_expiries), [None, 105])

    def test_plain_push_and_other_entries_also_allocate_receipts(self):
        # 旧入口行为不变（不返回回执），但内部同样占用回执号段
        self.assertTrue(self.cache.push('d1', 'e1', 10))
        r = self.cache.push_with_receipt('d2', 'e2', 10)
        self.assertEqual(r.receipt, 2)
        self.assertEqual([x.receipt for x in self.cache.peek_with_receipt()], [1, 2])

    def test_zero_event_ttl_still_gets_receipt(self):
        r = self.cache.push_expiring_with_receipt('d', 'e', 100, 0)
        self.assertEqual(dict(r), {'receipt': 1, 'accepted': True, 'reason': None})

    def test_receipts_not_reused_after_pop_or_cancel(self):
        self.cache.push_with_receipt('d1', 'e1', 10)
        self.cache.push_with_receipt('d2', 'e2', 10)
        self.cache.pop()          # 消费 1
        self.cache.cancel(2)      # 取消 2
        r = self.cache.push_with_receipt('d3', 'e3', 10)
        self.assertEqual(r.receipt, 3)  # 1、2 不复用

    # ---- 拒绝：receipt 为 None、reason 受限、不消耗号 ----
    def test_dedupe_window_rejection_has_none_receipt(self):
        self.cache.push_with_receipt('d', 'e1', 10)
        r = self.cache.push_with_receipt('d', 'e2', 10)
        self.assertEqual(dict(r), {'receipt': None, 'accepted': False,
                                   'reason': 'dedupe_window'})

    def test_queue_full_rejection_has_none_receipt(self):
        cache = self.make(max_queue=1)
        cache.push_with_receipt('d1', 'e1', 10)
        r = cache.push_with_receipt('d2', 'e2', 10)
        self.assertEqual(dict(r), {'receipt': None, 'accepted': False,
                                   'reason': 'queue_full'})

    def test_rejection_does_not_consume_receipt_number(self):
        cache = self.make(max_queue=1)
        cache.push_with_receipt('d1', 'e1', 10)               # 1
        cache.push_with_receipt('d2', 'e2', 10)               # queue_full
        cache.push_with_receipt('d1', 'dup', 10)              # dedupe_window
        cache.pop()
        self.assertEqual(cache.push_with_receipt('d3', 'e3', 10).receipt, 2)

    def test_receipt_push_validation_raises_before_clock(self):
        for bad in (-1, float('nan'), True, None, '10'):
            with self.assertRaises(ValueError):
                self.cache.push_with_receipt('d', 'e', bad)
        # event_ttl=0 合法（接受即到期）；非法的是负值/bool/None 等
        for bad in (-1, None, True, float('inf')):
            with self.assertRaises(ValueError):
                self.cache.push_expiring_with_receipt('d', 'e', 10, bad)
        self.assertEqual(self.clock_calls[0], 0)
        self.assertEqual(self.cache.queue_status().size, 0)

    # ---- drop_oldest：discarded 带驱逐回执 ----
    def test_drop_oldest_discarded_carries_evicted_receipt(self):
        cache = self.make(max_queue=1, overflow_policy='drop_oldest')
        r1 = cache.push_with_receipt('d1', 'e1', 10)
        r2 = cache.push_with_receipt('d2', 'e2', 10)
        self.assertEqual(r1.receipt, 1)
        self.assertEqual(len(r2.discarded), 1)
        self.assertEqual(dict(r2.discarded[0]),
                         {'event': 'e1', 'reason': 'queue_full', 'receipt': 1})
        self.assertEqual([cache.pop_with_receipt().receipt], [2])

    def test_drop_oldest_evicted_old_event_has_none_receipt(self):
        cache = self.make(max_queue=1, overflow_policy='drop_oldest')
        # 旧格式恢复：在队事件无回执
        cache.restore({
            'values': {}, 'events': ['old'], 'seen': {}, 'max_queue': 1,
            'overflow_policy': 'drop_oldest',
        })
        r = cache.push_with_receipt('d', 'new', 10)
        self.assertEqual(r.discarded[0],
                         Result(event='old', reason='queue_full', receipt=None))
        self.assertEqual(cache.peek_with_receipt(), [Result(event='new', receipt=1)])

    def test_old_push_with_reason_discarded_shape_unchanged(self):
        cache = self.make(max_queue=1, overflow_policy='drop_oldest')
        cache.push('d1', 'e1', 10)
        r = cache.push_with_reason('d2', 'e2', 10)
        self.assertEqual(set(r.discarded[0]), {'event', 'reason'})

    # ---- pop_with_receipt ----
    def test_pop_with_receipt_hit(self):
        self.cache.push_with_receipt('d1', 'e1', 10)
        self.cache.push('d2', None, 10)  # None 事件也以 found=True 返回
        self.assertEqual(self.cache.pop_with_receipt(),
                         Result(found=True, event='e1', receipt=1))
        r = self.cache.pop_with_receipt()
        self.assertEqual(r.found, True)
        self.assertIsNone(r.event)
        self.assertEqual(r.receipt, 2)

    def test_pop_with_receipt_empty_queue(self):
        self.assertEqual(self.cache.pop_with_receipt(),
                         Result(found=False, event=None, receipt=None))

    def test_pop_with_receipt_does_not_read_clock_or_judge_ttl(self):
        self.cache.push_expiring('d', 'e', 10, 0)  # 接受即到期
        self.advance(100)
        self.clock_calls[0] = 0
        r = self.cache.pop_with_receipt()  # 过期事件照样取出
        self.assertEqual(r, Result(found=True, event='e', receipt=1))
        self.assertEqual(self.clock_calls[0], 0)

    # ---- peek_with_receipt ----
    def test_peek_with_receipt_returns_fifo_results(self):
        self.cache.push_with_receipt('d1', 'a', 10)
        self.cache.push_expiring_with_receipt('d2', 'b', 10, 50)
        self.assertEqual(self.cache.peek_with_receipt(),
                         [Result(event='a', receipt=1), Result(event='b', receipt=2)])
        self.assertEqual(self.cache.queue_status().size, 2)  # 非破坏性

    def test_peek_with_receipt_empty_returns_empty_list(self):
        self.assertEqual(self.cache.peek_with_receipt(), [])

    def test_peek_with_receipt_does_not_read_clock(self):
        self.cache.push_expiring('d', 'e', 10, 0)
        self.advance(100)
        self.clock_calls[0] = 0
        self.assertEqual(self.cache.peek_with_receipt(), [Result(event='e', receipt=1)])
        self.assertEqual(self.clock_calls[0], 0)

    # ---- cancel：命中语义 ----
    def test_cancel_hit_removes_event_and_frees_slot(self):
        cache = self.make(max_queue=2)
        cache.push_with_receipt('d1', 'a', 10)
        cache.push_with_receipt('d2', 'b', 10)
        r = cache.cancel(1)
        self.assertEqual(dict(r), {'removed': True, 'event': 'a', 'reason': None})
        self.assertEqual(cache.pop_batch(), ['b'])  # 其余事件相对顺序不变
        self.assertTrue(cache.push('d3', 'c', 10))  # 容量已释放

    def test_cancel_middle_keeps_fifo_and_alignment(self):
        for i, ev in enumerate(['a', 'b', 'c']):
            self.cache.push_with_receipt('d%d' % i, ev, 10)
        self.assertEqual(self.cache.cancel(2).removed, True)
        self.assertEqual([x.event for x in self.cache.peek_with_receipt()], ['a', 'c'])
        self.assertEqual([x.receipt for x in self.cache.peek_with_receipt()], [1, 3])

    def test_cancel_ignores_event_ttl(self):
        # 取消不判 event_ttl：早已到期但仍在队中的事件照样取消
        self.cache.push_expiring('d', 'e', 100, 0)
        self.advance(500)
        r = self.cache.cancel(1)
        self.assertEqual(r, Result(removed=True, event='e', reason=None))

    def test_cancel_does_not_read_clock(self):
        self.cache.push_with_receipt('d', 'e', 10)
        self.clock_calls[0] = 0
        self.assertEqual(self.cache.cancel(1).removed, True)
        self.assertEqual(self.clock_calls[0], 0)
        self.assertEqual(self.cache.cancel(99).removed, False)
        self.assertEqual(self.clock_calls[0], 0)

    def test_cancel_frees_capacity_but_keeps_dedupe_window(self):
        cache = self.make(max_queue=1)
        cache.push('d', 'e', 100)  # 去重窗口到 200
        self.assertEqual(cache.cancel(1).removed, True)
        # 槽位释放但 seen 窗口保留：同去重键仍被拦截
        self.assertEqual(cache.push_with_reason('d', 'again', 100).reason,
                         'dedupe_window')
        self.assertIn('d', cache.seen)
        # 其他去重键可使用释放出的槽位
        self.assertTrue(cache.push('d2', 'new', 100))

    def test_cancel_does_not_write_discard_history(self):
        self.cache.push_with_receipt('d', 'e', 10)
        self.cache.cancel(1)
        self.assertEqual(self.cache.discard_history(), [])

    # ---- cancel：未命中（未知/已出队/已取消） ----
    def test_cancel_unknown_popped_cancelled_return_missing(self):
        self.cache.push_with_receipt('d1', 'a', 10)
        self.cache.push_with_receipt('d2', 'b', 10)
        missing = Result(removed=False, event=None, reason='missing')
        self.assertEqual(self.cache.cancel(999), missing)       # 未知
        self.cache.pop_with_receipt()                          # 1 已出队
        self.assertEqual(self.cache.cancel(1), missing)
        self.assertEqual(self.cache.cancel(2), Result(removed=True, event='b', reason=None))
        self.assertEqual(self.cache.cancel(2), missing)        # 已取消

    def test_cancel_old_none_receipt_event_misses(self):
        self.cache.restore({'values': {}, 'events': ['a'], 'seen': {}, 'max_queue': None})
        self.assertEqual(self.cache.cancel(1).reason, 'missing')

    # ---- cancel：receipt 校验原子性 ----
    def test_cancel_invalid_receipt_raises_value_error_atomically(self):
        self.cache.push_with_receipt('d', 'e', 10)
        self.cache.push_with_receipt('d2', 'f', 10)
        for bad in (0, -1, True, False, 1.5, 2.0, '1', None, 1j):
            with self.assertRaises(ValueError):
                self.cache.cancel(bad)
        self.assertEqual(self.clock_calls[0], 2)  # 只有两次 push 读过
        self.assertEqual([x.receipt for x in self.cache.peek_with_receipt()], [1, 2])

    # ---- apply_batch：回执相关操作与整批校验 ----
    def test_apply_batch_receipt_operations(self):
        results = self.cache.apply_batch([
            ('push_with_receipt', 'd1', 'e1', 10),
            ('push_expiring_with_receipt', 'd2', 'e2', 10, 50),
            ('peek_with_receipt',),
            ('cancel', 1),
            ('pop_with_receipt',),
        ])
        self.assertEqual(results[0], Result(receipt=1, accepted=True, reason=None))
        self.assertEqual(results[1].receipt, 2)
        self.assertEqual(results[2],
                         [Result(event='e1', receipt=1), Result(event='e2', receipt=2)])
        self.assertEqual(results[3], Result(removed=True, event='e1', reason=None))
        self.assertEqual(results[4], Result(found=True, event='e2', receipt=2))

    def test_apply_batch_receipt_push_rejection_shape(self):
        results = self.cache.apply_batch([
            ('push_with_receipt', 'd', 'e1', 10),
            ('push_with_receipt', 'd', 'e2', 10),
        ])
        self.assertEqual(results[0].receipt, 1)
        self.assertEqual(results[1],
                         Result(receipt=None, accepted=False, reason='dedupe_window'))

    def test_apply_batch_invalid_receipt_rolls_back_entire_batch(self):
        self.cache.put('k', 'v', 100)
        before = (dict(self.cache.values), list(self.cache.events), dict(self.cache.seen))
        for bad, exc in (
            ([('push_with_receipt', 'd', 'e', 10), ('cancel', 0)], ValueError),
            ([('push_with_receipt', 'd', 'e', -1)], ValueError),
            ([('cancel', True)], ValueError),
            ([('pop_with_receipt', 1)], ValueError),
        ):
            calls_before = self.clock_calls[0]
            with self.assertRaises(exc):
                self.cache.apply_batch(bad)
            self.assertEqual(self.clock_calls[0], calls_before)
        self.assertEqual(
            (dict(self.cache.values), list(self.cache.events), dict(self.cache.seen)),
            before)

    # ---- replay_batch：回执按记录时间判定、不读注入时钟 ----
    def test_replay_batch_receipt_operations_use_record_time(self):
        results = self.cache.replay_batch([
            (10, ('push_with_receipt', 'd1', 'e1', 10)),       # 窗口到 20
            (15, ('push_with_receipt', 'd1', 'dup', 10)),      # dedupe_window
            (20, ('push_expiring_with_receipt', 'd2', 'ex', 100, 0)),
            (20, ('peek_with_receipt',)),
            (30, ('cancel', 2)),                               # 不判 TTL，照样取消
            (30, ('pop_with_receipt',)),
            (30, ('pop_with_receipt',)),
        ])
        self.assertEqual(self.clock_calls[0], 0)  # 全程不读注入时钟
        self.assertEqual(results[0], Result(receipt=1, accepted=True, reason=None))
        self.assertEqual(results[1],
                         Result(receipt=None, accepted=False, reason='dedupe_window'))
        self.assertEqual(results[2].receipt, 2)  # 拒绝未消耗号
        self.assertEqual(results[3],
                         [Result(event='e1', receipt=1), Result(event='ex', receipt=2)])
        self.assertEqual(results[4], Result(removed=True, event='ex', reason=None))
        self.assertEqual(results[5], Result(found=True, event='e1', receipt=1))
        self.assertEqual(results[6], Result(found=False, event=None, receipt=None))

    def test_replay_batch_invalid_receipt_rolls_back(self):
        with self.assertRaises(ValueError):
            self.cache.replay_batch([
                (1, ('push_with_receipt', 'd', 'e', 10)),
                (2, ('cancel', 0)),
            ])
        self.assertEqual(self.clock_calls[0], 0)
        self.assertEqual(self.cache.queue_status().size, 0)

    # ---- 快照：对齐回执与计数器 ----
    def test_snapshot_includes_aligned_receipts_and_next_receipt(self):
        self.cache.push_with_receipt('d1', 'a', 10)
        self.cache.push_with_receipt('d2', 'b', 10)
        snap = self.cache.snapshot()
        self.assertEqual(snap.event_receipts, [1, 2])
        self.assertEqual(snap.next_receipt, 3)
        self.assertIsInstance(snap.event_receipts, list)

    def test_snapshot_keeps_counter_when_queue_emptied(self):
        self.cache.push_with_receipt('d1', 'a', 10)
        self.cache.cancel(1)
        snap = self.cache.snapshot()
        self.assertEqual(snap.event_receipts, [])
        self.assertEqual(snap.next_receipt, 2)  # 空队列也保留号段

    def test_restore_continues_numbering_with_reuse_prevention(self):
        self.cache.push_with_receipt('d1', 'a', 10)
        self.cache.push_with_receipt('d2', 'b', 10)
        self.cache.pop_with_receipt()
        target = EventCache(lambda: 0)
        target.restore(self.cache.snapshot())
        self.assertEqual(target.pop_with_receipt(),
                         Result(found=True, event='b', receipt=2))
        self.assertEqual(target.push_with_receipt('d3', 'c', 10).receipt, 3)

    def test_restore_receipt_lists_detached_from_snapshot(self):
        self.cache.push_with_receipt('d', 'e', 10)
        snap = self.cache.snapshot()
        target = EventCache(lambda: 0)
        target.restore(snap)
        snap['event_receipts'][0] = 99
        self.assertEqual(list(target.event_receipts), [1])

    def test_restore_old_snapshot_gives_none_receipts_and_counter_one(self):
        self.cache.restore(
            {'values': {}, 'events': ['a', 'b'], 'seen': {}, 'max_queue': None})
        self.assertEqual(list(self.cache.event_receipts), [None, None])
        self.assertEqual(self.cache.snapshot().next_receipt, 1)
        self.assertEqual(self.cache.peek_with_receipt(),
                         [Result(event='a', receipt=None),
                          Result(event='b', receipt=None)])
        self.assertEqual(self.cache.cancel(1).reason, 'missing')
        self.assertEqual(self.cache.push_with_receipt('d', 'c', 10).receipt, 1)

    def test_restore_rejects_inconsistent_receipt_snapshot(self):
        base = {'values': {}, 'events': ['e'], 'seen': {}, 'max_queue': None}

        def expect_value_error(extra):
            cache = self.make()
            with self.assertRaises(ValueError):
                cache.restore(dict(base, **extra))

        expect_value_error({'event_receipts': [0], 'next_receipt': 2})
        expect_value_error({'event_receipts': [True], 'next_receipt': 2})
        expect_value_error({'event_receipts': [1.5], 'next_receipt': 2})
        expect_value_error({'event_receipts': [5], 'next_receipt': 5})   # 须严格大于
        expect_value_error({'event_receipts': [5], 'next_receipt': 3})
        expect_value_error({'event_receipts': [1, 2], 'next_receipt': 3})  # 长度不符
        expect_value_error({'event_receipts': [1]})                        # 缺一
        expect_value_error({'next_receipt': 1})                            # 缺一
        expect_value_error({'event_receipts': [None], 'next_receipt': True})

    def test_restore_accepts_all_none_receipts(self):
        cache = self.make()
        cache.restore({
            'values': {}, 'events': ['a', 'b'], 'seen': {}, 'max_queue': None,
            'event_receipts': [None, None], 'next_receipt': 1,
        })
        self.assertEqual(list(cache.event_receipts), [None, None])

    def test_failed_restore_keeps_receipt_state_functional(self):
        self.cache.push_with_receipt('d', 'e', 10)
        with self.assertRaises(ValueError):
            self.cache.restore({
                'values': {}, 'events': ['x'], 'seen': {}, 'max_queue': None,
                'event_receipts': [5], 'next_receipt': 5,
            })
        self.assertEqual(self.cache.pop_with_receipt(),
                         Result(found=True, event='e', receipt=1))

    # ---- restore：None 占位、编号间隔与严格递增/唯一性边界 ----
    def test_restore_accepts_none_placeholders_and_gaps(self):
        # 允许 None 占位与历史取消/出队/驱逐造成的编号间隔；过滤 None 后
        # 只需唯一严格递增，编号无需连续
        cache = self.make()
        cache.restore({
            'values': {},
            'events': ['a', 'b', 'c', 'd'],
            'seen': {}, 'max_queue': None,
            'event_receipts': [None, 3, None, 7],
            'next_receipt': 9,
        })
        self.assertEqual(list(cache.event_receipts), [None, 3, None, 7])
        self.assertEqual(cache.snapshot().next_receipt, 9)
        # None 占位旧事件不可 cancel；有号事件按原回执命中
        self.assertEqual(cache.cancel(3),
                         Result(removed=True, event='b', reason=None))
        self.assertEqual(cache.cancel(1).reason, 'missing')
        # 新事件只从 next_receipt 分配，间隔号（1、2、4、5、6、8）不复用
        self.assertEqual(cache.push_with_receipt('d', 'e', 10).receipt, 9)

    def test_restore_requires_strictly_increasing_across_none_placeholders(self):
        # 跨 None 占位仍须严格递增：占位不重置比较基准
        cache = self.make()
        with self.assertRaises(ValueError):
            cache.restore({
                'values': {}, 'events': ['a', 'b', 'c'],
                'seen': {}, 'max_queue': None,
                'event_receipts': [5, None, 4], 'next_receipt': 6,
            })
        self.assertEqual(list(cache.events), [])

    def test_restore_rejects_duplicate_receipts(self):
        for receipts in ([2, 2], [None, 4, 4], [3, None, 3]):
            cache = self.make()
            with self.assertRaises(ValueError):
                cache.restore({
                    'values': {}, 'events': ['e'] * len(receipts),
                    'seen': {}, 'max_queue': None,
                    'event_receipts': receipts,
                    'next_receipt': max(r for r in receipts if r is not None) + 1,
                })
            self.assertEqual(list(cache.events), [])

    def test_restore_rejects_out_of_order_receipts(self):
        for receipts in ([2, 1], [1, 3, 2], [3, None, 2]):
            cache = self.make()
            with self.assertRaises(ValueError):
                cache.restore({
                    'values': {}, 'events': ['e'] * len(receipts),
                    'seen': {}, 'max_queue': None,
                    'event_receipts': receipts,
                    'next_receipt': max(r for r in receipts if r is not None) + 1,
                })
            self.assertEqual(list(cache.events), [])

    def test_restore_real_snapshot_with_cancel_gap_preserves_sequence(self):
        # 真实操作路径：取消中间事件后在队回执天然带编号间隔 [1, 3, 4]
        for i, key in enumerate(('d1', 'd2', 'd3', 'd4'), start=1):
            self.cache.push_with_receipt(key, 'e%d' % i, 100)
        self.cache.cancel(2)
        snap = self.cache.snapshot()
        self.assertEqual(snap.event_receipts, [1, 3, 4])
        self.assertEqual(snap.next_receipt, 5)
        target = self.make()
        target.restore(snap)
        self.assertEqual([x.receipt for x in target.peek_with_receipt()], [1, 3, 4])
        self.assertEqual(target.push_with_receipt('d5', 'e5', 10).receipt, 5)

    def test_restore_round_trip_preserves_gaps_placeholders_order_and_counter(self):
        # 含间隔与 None 占位的合法快照经 snapshot 再恢复应原样保留
        crafted = {
            'values': {},
            'events': ['a', 'b', 'c'],
            'seen': {}, 'max_queue': None,
            'event_receipts': [None, 4, 9],
            'next_receipt': 11,
        }
        first = self.make()
        first.restore(crafted)
        snap = first.snapshot()
        self.assertEqual(snap.event_receipts, [None, 4, 9])
        self.assertEqual(snap.next_receipt, 11)
        second = self.make()
        second.restore(snap)
        self.assertEqual(list(second.event_receipts), [None, 4, 9])
        self.assertEqual(second.snapshot().next_receipt, 11)
        self.assertEqual([x.event for x in second.peek_with_receipt()],
                         ['a', 'b', 'c'])

    def test_restore_then_push_never_reuses_skipped_or_cancelled_numbers(self):
        cache = self.make()
        cache.restore({
            'values': {}, 'events': ['a'],
            'seen': {}, 'max_queue': None,
            'event_receipts': [4], 'next_receipt': 6,  # 1-3 与 5 均不可复用
        })
        self.assertEqual(cache.cancel(4).removed, True)
        self.assertEqual(cache.push_with_receipt('d1', 'b', 10).receipt, 6)
        self.assertEqual(
            cache.push_expiring_with_receipt('d2', 'c', 10, 5).receipt, 7)
        self.assertEqual(cache.snapshot().next_receipt, 8)

    def test_failed_ordering_restore_is_atomic_and_does_not_read_clock(self):
        self.cache.push_with_receipt('d1', 'e1', 10)
        self.cache.push_with_receipt('d2', 'e2', 10)
        before = (list(self.cache.events),
                  list(self.cache.event_receipts),
                  self.cache._next_receipt)
        calls_before = self.clock_calls[0]
        with self.assertRaises(ValueError):
            self.cache.restore({
                'values': {}, 'events': ['a', 'b'],
                'seen': {}, 'max_queue': None,
                'event_receipts': [2, 1], 'next_receipt': 3,
            })
        # 校验完成前不读取注入时钟、不写入任何字段
        self.assertEqual(self.clock_calls[0], calls_before)
        after = (list(self.cache.events),
                 list(self.cache.event_receipts),
                 self.cache._next_receipt)
        self.assertEqual(before, after)
        # 原状态仍遵守既有回执语义：1 可取消，新号从 3 分配
        self.assertEqual(self.cache.cancel(1).removed, True)
        self.assertEqual(self.cache.push_with_receipt('d3', 'e3', 10).receipt, 3)

    # ---- 各清理/驱逐路径保持回执对齐 ----
    def test_cleanup_and_discard_keep_receipts_aligned(self):
        self.cache.push_expiring('d1', 'x1', 100, 0)  # 回执 1，到期
        self.cache.push('d2', 'live', 100)            # 回执 2
        self.cache.push_expiring('d3', 'x2', 100, 0)  # 回执 3，到期
        self.advance(50)
        self.cache.cleanup_expired_events()
        self.assertEqual([x.receipt for x in self.cache.peek_with_receipt()], [2])
        self.assertEqual([x.event for x in self.cache.peek_with_receipt()], ['live'])

    def test_resize_shrink_keeps_tail_receipts_aligned(self):
        cache = self.make(max_queue=3)
        cache.push_with_receipt('d1', 'a', 10)
        cache.push_with_receipt('d2', 'b', 10)
        result = cache.resize_queue(1)  # 挤出 a（回执 1）
        self.assertEqual([x.receipt for x in cache.peek_with_receipt()], [2])
        # resize 的 discarded 形状保持既有两字段，不携带回执
        self.assertEqual(set(result.discarded[0]), {'event', 'reason'})
        self.assertEqual(result.discarded[0].event, 'a')

    def test_pop_live_batch_keeps_remaining_receipts_aligned(self):
        self.cache.push_expiring('d1', 'x', 100, 0)
        self.cache.push('d2', 'a', 100)
        self.advance(50)
        self.cache.pop_live_batch()
        self.assertEqual(self.cache.peek_with_receipt(), [])


class InspectTest(unittest.TestCase):
    FIELDS = {
        'observed_at', 'value_count', 'expired_value_count', 'dedupe_count',
        'expired_dedupe_count', 'queue_size', 'live_event_count',
        'expired_event_count', 'next_value_expiry', 'next_dedupe_expiry',
        'next_event_expiry', 'discard_history_size',
    }

    def setUp(self):
        self.now = [100]
        self.clock_calls = [0]

        def clock():
            self.clock_calls[0] += 1
            return self.now[0]

        self.clock = clock
        self.cache = EventCache(clock)

    def advance(self, seconds):
        self.now[0] += seconds

    def fill_mixed_state(self):
        # values：一条存活（到期点 110）、一条零 TTL 同刻到期但仍占容器
        self.cache.put('v_live', 'a', 10)
        self.cache.put('v_dead', 'b', 0)
        # seen：d_dead 窗口到 100（同刻到期），d_live 窗口到 120（存活）
        self.cache.push('d_dead', 'e_no_ttl', 0)
        # 事件：e_no_ttl 无事件 TTL；e_live 到期点 108；d 窗口到 120
        self.cache.push_expiring('d_live', 'e_live', 20, 8)

    # ---- 返回形状 ----
    def test_empty_cache_returns_all_fields(self):
        result = self.cache.inspect()
        self.assertIsInstance(result, Result)
        self.assertEqual(set(result), self.FIELDS)
        self.assertEqual(result.observed_at, 100)
        self.assertEqual(result.value_count, 0)
        self.assertEqual(result.expired_value_count, 0)
        self.assertEqual(result.dedupe_count, 0)
        self.assertEqual(result.expired_dedupe_count, 0)
        self.assertEqual(result.queue_size, 0)
        self.assertEqual(result.live_event_count, 0)
        self.assertEqual(result.expired_event_count, 0)
        self.assertIsNone(result.next_value_expiry)
        self.assertIsNone(result.next_dedupe_expiry)
        self.assertIsNone(result.next_event_expiry)
        self.assertEqual(result.discard_history_size, 0)

    def test_reads_clock_exactly_once(self):
        before = self.clock_calls[0]
        self.cache.inspect()
        self.assertEqual(self.clock_calls[0] - before, 1)
        # 空容器也读取一次时钟
        EventCache(self.clock).inspect()

    def test_mixed_state_counts_and_next_expiries(self):
        self.fill_mixed_state()
        result = self.cache.inspect()
        self.assertEqual(result.observed_at, 100)
        self.assertEqual((result.value_count, result.expired_value_count), (1, 1))
        self.assertEqual((result.dedupe_count, result.expired_dedupe_count), (1, 1))
        self.assertEqual(result.queue_size, 2)
        self.assertEqual((result.live_event_count, result.expired_event_count), (2, 0))
        self.assertEqual(result.next_value_expiry, 110)
        self.assertEqual(result.next_dedupe_expiry, 120)
        self.assertEqual(result.next_event_expiry, 108)
        self.assertEqual(result.discard_history_size, 0)

    def test_event_counts_sum_to_queue_size(self):
        self.fill_mixed_state()
        self.advance(8)  # e_live 在 108 恰好到期
        result = self.cache.inspect()
        self.assertEqual(result.queue_size, 2)
        self.assertEqual(result.live_event_count, 1)   # 无 TTL 事件始终存活
        self.assertEqual(result.expired_event_count, 1)
        self.assertIsNone(result.next_event_expiry)
        self.assertEqual(
            result.live_event_count + result.expired_event_count, result.queue_size)

    def test_events_without_ttl_always_live_and_have_no_expiry(self):
        self.cache.push('d1', 'a', 100)
        self.cache.push('d2', 'b', 100)
        self.advance(1000)
        result = self.cache.inspect()
        self.assertEqual(result.queue_size, 2)
        self.assertEqual(result.live_event_count, 2)
        self.assertEqual(result.expired_event_count, 0)
        self.assertIsNone(result.next_event_expiry)

    def test_next_expiry_is_earliest_strictly_after_observed_at(self):
        self.cache.put('a', 1, 30)   # 130
        self.cache.put('b', 2, 10)   # 110
        self.cache.put('c', 3, 20)   # 120
        result = self.cache.inspect()
        self.assertEqual(result.next_value_expiry, 110)
        self.advance(10)  # 到达 110：b 恰好到期，下一个最早存活到期点为 120
        result = self.cache.inspect()
        self.assertEqual(result.expired_value_count, 1)
        self.assertEqual(result.value_count, 2)
        self.assertEqual(result.next_value_expiry, 120)

    def test_dedupe_and_history_sizes(self):
        cache = EventCache(self.clock, max_queue=1, overflow_policy='drop_oldest',
                           discard_history_limit=5)
        cache.push_with_receipt('d1', 'x', 100)
        cache.push_with_receipt('d2', 'y', 100)  # 挤出 x，写一条 queue_full 历史
        result = cache.inspect()
        self.assertEqual(result.queue_size, 1)
        self.assertEqual(result.dedupe_count, 2)
        self.assertEqual(result.discard_history_size, 1)

    # ---- 只读纯度 ----
    def test_inspect_is_pure_and_does_not_evict_expired_items(self):
        self.fill_mixed_state()
        self.cache.inspect()
        self.cache.inspect()
        # 到期项仍占据各自容器，未被 inspect 清理
        self.assertIn('v_dead', self.cache.values)
        self.assertIn('d_dead', self.cache.seen)
        self.assertEqual(list(self.cache.events), ['e_no_ttl', 'e_live'])
        self.assertEqual(list(self.cache.event_expiries), [None, 108])
        self.assertEqual(list(self.cache.event_receipts), [1, 2])
        self.assertEqual(self.cache._next_receipt, 3)
        self.assertEqual(self.cache.discard_history(), [])

    def test_inspect_does_not_change_subsequent_operations(self):
        twin = EventCache(self.clock)
        for cache in (self.cache, twin):
            cache.put('v_live', 'a', 10)
            cache.put('v_dead', 'b', 0)
            cache.push('d_dead', 'e_no_ttl', 0)
            cache.push_expiring('d_live', 'e_live', 20, 8)
        # 一个实例多次 inspect，另一个不调用
        for _ in range(4):
            self.cache.inspect()
        self.advance(8)
        self.assertEqual(self.cache.cleanup(), twin.cleanup())
        self.assertEqual(self.cache.discard_expired_events(),
                         twin.discard_expired_events())
        self.assertEqual(self.cache.pop_batch(), twin.pop_batch())
        self.assertEqual(self.cache.discard_history(), twin.discard_history())

    def test_inspect_does_not_advance_state_in_reject_regression_failure(self):
        now_box = [10]

        def fixed_clock():
            return now_box[0]

        cache = EventCache(fixed_clock, clock_policy='reject_regression')
        cache.put('k', 'v', 100)  # 成功采样，水位 10
        now_box[0] = 5
        with self.assertRaises(app.ClockRegressionError):
            cache.inspect()
        # 抛错后水位与数据都保持原样
        self.assertEqual(cache.clock_status().last_time, 10)
        self.assertEqual(cache.values['k'], ('v', 110))

    def test_clock_exception_propagates_unchanged(self):
        class Boom(Exception):
            pass

        def bad_clock():
            raise Boom()

        cache = EventCache(bad_clock)
        cache.values['k'] = ('v', 200)
        with self.assertRaises(Boom):
            cache.inspect()
        self.assertEqual(cache.values['k'], ('v', 200))

    # ---- apply_batch ----
    def test_apply_batch_inspect_reuses_batch_clock_and_reflects_prior_ops(self):
        before = self.clock_calls[0]
        results = self.cache.apply_batch([
            ('put', 'a', 1, 10),
            ('put', 'b', 2, 0),                       # 同刻到期但保留
            ('push', 'd0', 'e0', 5),                  # seen 到 105
            ('push_expiring', 'd1', 'e1', 50, 8),     # 事件到期点 108
            ('inspect',),
            ('cleanup',),
        ])
        self.assertEqual(self.clock_calls[0] - before, 1)  # 整批仍只读一次时钟
        inspected = results[4]
        self.assertEqual(set(inspected), self.FIELDS)
        self.assertEqual(inspected.observed_at, 100)
        self.assertEqual((inspected.value_count, inspected.expired_value_count), (1, 1))
        self.assertEqual(inspected.dedupe_count, 2)
        self.assertEqual((inspected.live_event_count, inspected.expired_event_count), (2, 0))
        self.assertEqual(inspected.next_value_expiry, 110)
        self.assertEqual(inspected.next_dedupe_expiry, 105)
        self.assertEqual(inspected.next_event_expiry, 108)
        # inspect 没有提前清理：随后的 cleanup 真正移除到期值
        self.assertEqual(results[5].values_removed, 1)

    def test_apply_batch_inspect_does_not_affect_later_operations(self):
        results = self.cache.apply_batch([
            ('put', 'a', 1, 10),
            ('push', 'd0', 'e0', 5),
            ('inspect',),
            ('push', 'd0', 'dup', 5),  # 窗口仍在：inspect 未续期/清理 seen
            ('inspect',),
        ])
        self.assertFalse(results[3].accepted)
        self.assertEqual(results[3].reason, 'dedupe_window')
        self.assertEqual(results[4].queue_size, 1)

    def test_apply_batch_invalid_inspect_structure_raises_value_error(self):
        self.fill_mixed_state()
        before = (dict(self.cache.values), list(self.cache.events),
                  list(self.cache.event_expiries), dict(self.cache.seen))
        calls_before = self.clock_calls[0]
        for bad in (('inspect', 1), ('inspect', None), ('inspect', 'x'),
                    ('inspect', 1, 2)):
            with self.assertRaises(ValueError):
                self.cache.apply_batch([bad])
        self.assertEqual(self.clock_calls[0], calls_before)
        self.assertEqual(
            (dict(self.cache.values), list(self.cache.events),
             list(self.cache.event_expiries), dict(self.cache.seen)), before)

    # ---- replay_batch ----
    def test_replay_inspect_uses_record_timestamp_without_instance_clock(self):
        instance_calls = [0]

        def must_not_be_called():
            instance_calls[0] += 1
            return 999999

        cache = EventCache(must_not_be_called)
        records = [
            (100, ('put', 'a', 1, 10)),                 # 到期点 110
            (100, ('push_expiring', 'd', 'e', 50, 5)),  # 事件到期点 105
            (105, ('inspect',)),
            (105, ('pop',)),                # 到期事件仍在队：inspect 未移除
            (110, ('inspect',)),
        ]
        results = cache.replay_batch(records)
        self.assertEqual(instance_calls[0], 0)
        first = results[2]
        self.assertEqual(first.observed_at, 105)
        self.assertEqual(first.queue_size, 1)
        self.assertEqual((first.live_event_count, first.expired_event_count), (0, 1))
        self.assertIsNone(first.next_event_expiry)
        self.assertEqual(first.value_count, 1)
        self.assertEqual(results[3], 'e')
        second = results[4]
        self.assertEqual(second.observed_at, 110)
        self.assertEqual((second.value_count, second.expired_value_count), (0, 1))
        self.assertIsNone(second.next_value_expiry)

    def test_replay_inspect_is_deterministic_and_does_not_change_later_records(self):
        records = [
            (0, ('put', 'a', 1, 10)),
            (0, ('push_expiring', 'd1', 'e1', 20, 5)),
            (3, ('inspect',)),
            (3, ('push', 'd1', 'dup', 20)),  # 窗口内仍被去重
            (5, ('inspect',)),                # e1 恰好到期但仍占槽位
            (5, ('pop',)),
        ]

        def run():
            calls = [0]
            cache = EventCache(lambda: calls.__setitem__(0, calls[0] + 1) or 0)
            results = cache.replay_batch(records)
            return calls[0], results

        calls_a, results_a = run()
        _calls_b, results_b = run()
        self.assertEqual(calls_a, 0)  # 回放不读取实例时钟

        def normalize(value):
            if isinstance(value, dict):
                return {k: normalize(v) for k, v in value.items()}
            if isinstance(value, list):
                return [normalize(v) for v in value]
            return value

        self.assertEqual(normalize(results_a), normalize(results_b))
        self.assertFalse(results_a[3].accepted)
        self.assertEqual(results_a[3].reason, 'dedupe_window')
        self.assertEqual(results_a[4].expired_event_count, 1)
        self.assertEqual(results_a[4].queue_size, 1)
        self.assertEqual(results_a[5], 'e1')

    def test_replay_invalid_inspect_record_raises_value_error(self):
        for bad in (
            [(1, ('inspect', 1))],       # 结构非法
            [('x', ('inspect',))],       # 时间戳非法
            [(2, ('inspect',)), (1, ('inspect',))],  # 时间戳倒退
            [(1, ('unknown',))],
        ):
            with self.assertRaises(ValueError):
                EventCache(self.clock).replay_batch(bad)


class QueueMetadataTest(unittest.TestCase):
    def setUp(self):
        self.now = [100]
        self.clock_calls = [0]

        def clock():
            self.clock_calls[0] += 1
            return self.now[0]

        self.clock = clock
        self.cache = EventCache(clock)

    def make(self, **kwargs):
        return EventCache(self.clock, **kwargs)

    # ---- 基本形状与字段 ----
    def test_peek_with_metadata_reports_push_entries(self):
        self.cache.push('d1', 'e1', 10)                    # 去重窗口到 110
        self.cache.push_expiring('d2', 'e2', 20, 50)       # 去重 120，事件 150
        items = self.cache.peek_with_metadata()
        self.assertIsInstance(items, list)
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0], Result(
            event='e1', dedupe_key='d1', dedupe_expires_at=110,
            event_expires_at=None, receipt=1, dedupe_known=True))
        self.assertEqual(items[1], Result(
            event='e2', dedupe_key='d2', dedupe_expires_at=120,
            event_expires_at=150, receipt=2, dedupe_known=True))
        self.assertIsInstance(items[0], Result)

    def test_metadata_distinguishes_real_none_key_from_unknown(self):
        self.cache.push(None, 'e1', 10)  # 真实的 None 去重键
        item = self.cache.peek_with_metadata()[0]
        self.assertIsNone(item.dedupe_key)
        self.assertIs(item.dedupe_known, True)
        self.assertEqual(item.dedupe_expires_at, 110)

    def test_metadata_fields_track_receipt_time_not_later_reads(self):
        self.cache.push('d', 'e', 10)
        self.now[0] = 500  # 窗口与事件判定时刻早已过去，元数据仍记接收时刻
        item = self.cache.peek_with_metadata()[0]
        self.assertEqual(item.dedupe_expires_at, 110)

    # ---- limit 语义与 peek/pop_batch 一致 ----
    def test_peek_with_metadata_limit_prefix(self):
        for i in range(3):
            self.cache.push('d%d' % i, 'e%d' % i, 10)
        self.assertEqual([m.event for m in self.cache.peek_with_metadata(2)],
                         ['e0', 'e1'])
        self.assertEqual([m.event for m in self.cache.peek_with_metadata(99)],
                         ['e0', 'e1', 'e2'])
        self.assertEqual(self.cache.peek_with_metadata(0), [])
        self.assertEqual(len(self.cache.events), 3)  # 纯查看不移除

    def test_peek_with_metadata_empty_queue(self):
        self.assertEqual(self.cache.peek_with_metadata(), [])
        self.assertEqual(self.cache.peek_with_metadata(0), [])

    def test_metadata_limit_validation_no_clock_no_state(self):
        self.cache.push('d', 'e', 10)
        for fn in (self.cache.peek_with_metadata, self.cache.pop_with_metadata):
            for bad in (-1, 2.5, '1', True, object()):
                calls_before = self.clock_calls[0]
                with self.assertRaises(ValueError):
                    fn(bad)
                self.assertEqual(self.clock_calls[0], calls_before)
                self.assertEqual(list(self.cache.events), ['e'])

    # ---- 纯查询/纯出队：不读时钟、不清理、不写历史 ----
    def test_peek_with_metadata_reads_no_clock_and_keeps_expired(self):
        self.cache.push_expiring('d', 'e', 10, 5)  # 事件到期点 105
        self.now[0] = 1000
        calls_before = self.clock_calls[0]
        items = self.cache.peek_with_metadata()
        self.assertEqual(self.clock_calls[0], calls_before)
        self.assertEqual(len(items), 1)            # 过期事件原样报告
        self.assertEqual(items[0].event_expires_at, 105)
        self.assertEqual(len(self.cache.events), 1)  # 不触发清理
        self.assertEqual(self.cache.discard_history(), [])

    def test_pop_with_metadata_takes_fifo_prefix_and_frees_capacity(self):
        cache = self.make(max_queue=2)
        cache.push('d1', 'e1', 10)
        cache.push('d2', 'e2', 20)
        self.assertEqual(cache.push_with_reason('d3', 'e3', 10).reason,
                         'queue_full')
        taken = cache.pop_with_metadata(1)
        self.assertEqual([m.event for m in taken], ['e1'])
        self.assertEqual(taken[0].dedupe_key, 'd1')
        self.assertEqual(taken[0].dedupe_expires_at, 110)
        self.assertEqual(taken[0].receipt, 1)
        self.assertIs(taken[0].dedupe_known, True)
        # 释放容量后可继续入队；剩余事件保持 FIFO 顺序
        self.assertTrue(cache.push('d3', 'e3', 10))
        self.assertEqual(cache.peek(), ['e2', 'e3'])

    def test_pop_with_metadata_default_takes_all(self):
        self.cache.push('d1', 'e1', 10)
        self.cache.push('d2', 'e2', 10)
        self.assertEqual([m.event for m in self.cache.pop_with_metadata()],
                         ['e1', 'e2'])
        self.assertEqual(self.cache.pop_with_metadata(), [])
        self.assertEqual(len(self.cache.event_metadata), 0)

    def test_pop_with_metadata_keeps_seen_and_writes_no_history(self):
        self.cache.push('d', 'e', 100)
        self.cache.pop_with_metadata()
        # seen 去重窗口保留：窗口内同键仍被去重拦截
        self.assertEqual(self.cache.seen, {'d': 200})
        self.assertEqual(self.cache.push_with_reason('d', 'e2', 10).reason,
                         'dedupe_window')
        self.assertEqual(self.cache.discard_history(), [])

    def test_pop_with_metadata_reads_no_clock_for_expired_events(self):
        self.cache.push_expiring('d', 'e', 10, 5)
        self.now[0] = 1000
        calls_before = self.clock_calls[0]
        items = self.cache.pop_with_metadata()
        self.assertEqual(self.clock_calls[0], calls_before)
        self.assertEqual([m.event for m in items], ['e'])
        self.assertEqual(items[0].event_expires_at, 105)
        self.assertEqual(self.cache.discard_history(), [])

    # ---- 对齐性：各出队/清理路径同步维护元数据 ----
    def test_metadata_stays_aligned_through_queue_mutations(self):
        cache = self.make(max_queue=3, overflow_policy='drop_oldest')
        cache.push_expiring('a', 'e1', 10, 0)   # 接受时已到期
        cache.push('b', 'e2', 10)
        cache.push('c', 'e3', 10)
        cache.push('d', 'e4', 10)               # drop_oldest 挤出 e1
        cache.cancel(2)                         # 取消 e2
        cache.push_expiring('e', 'e5', 10, 0)
        self.now[0] = 200
        cache.cleanup_expired_events()          # 清掉 e5
        self.assertEqual([m.event for m in cache.peek_with_metadata()],
                         cache.peek())
        self.assertEqual([m.dedupe_key for m in cache.peek_with_metadata()],
                         ['c', 'd'])
        cache.resize_queue(1)                   # 挤出 e3
        self.assertEqual([m.dedupe_key for m in cache.peek_with_metadata()],
                         ['d'])
        cache.pop()
        self.assertEqual(cache.peek_with_metadata(), [])
        self.assertEqual(len(cache.event_metadata), 0)

    # ---- 快照：始终携带对齐的 event_metadata ----
    def test_snapshot_includes_aligned_event_metadata(self):
        self.cache.push('d1', 'e1', 10)
        self.cache.push_expiring('d2', 'e2', 20, 50)
        snap = self.cache.snapshot()
        self.assertIn('event_metadata', snap)
        self.assertIsInstance(snap.event_metadata, list)
        self.assertEqual(len(snap.event_metadata), len(snap.events))
        self.assertEqual(snap.event_metadata[0], Result(
            dedupe_known=True, dedupe_key='d1', dedupe_expires_at=110))
        self.assertEqual(snap.event_metadata[1], Result(
            dedupe_known=True, dedupe_key='d2', dedupe_expires_at=120))

    def test_snapshot_metadata_detached_from_cache(self):
        self.cache.push('d', 'e', 10)
        snap = self.cache.snapshot()
        snap['event_metadata'][0]['dedupe_key'] = 'hacked'
        snap['event_metadata'].append(Result(
            dedupe_known=True, dedupe_key='x', dedupe_expires_at=1))
        self.assertEqual(self.cache.peek_with_metadata()[0].dedupe_key, 'd')
        self.assertEqual(len(self.cache.event_metadata), 1)

    def test_snapshot_restore_round_trip_preserves_metadata(self):
        self.cache.push('d1', 'e1', 10)
        self.cache.push_expiring(None, 'e2', 20, 50)
        snap1 = self.cache.snapshot()
        rebuilt = self.make()
        self.assertIsNone(rebuilt.restore(snap1))
        self.assertEqual(rebuilt.snapshot(), snap1)
        self.assertEqual(rebuilt.peek_with_metadata(),
                         self.cache.peek_with_metadata())

    def test_restore_old_snapshot_marks_metadata_unknown(self):
        self.cache.restore({'values': {}, 'events': ['a', 'b'],
                            'seen': {}, 'max_queue': None})
        items = self.cache.peek_with_metadata()
        self.assertEqual([m.event for m in items], ['a', 'b'])
        for item in items:
            self.assertIs(item.dedupe_known, False)
            self.assertIsNone(item.dedupe_key)
            self.assertIsNone(item.dedupe_expires_at)
            self.assertIsNone(item.event_expires_at)
            self.assertIsNone(item.receipt)
        # 恢复后再快照仍往返一致
        snap = self.cache.snapshot()
        rebuilt = self.make()
        rebuilt.restore(snap)
        self.assertEqual(rebuilt.snapshot(), snap)

    def test_restore_metadata_unknown_entries_normalize_fields_to_none(self):
        self.cache.restore({
            'values': {}, 'events': ['a'], 'seen': {}, 'max_queue': None,
            'event_metadata': [{'dedupe_known': False,
                                'dedupe_key': 'ignored',
                                'dedupe_expires_at': 5}]})
        item = self.cache.peek_with_metadata()[0]
        self.assertIs(item.dedupe_known, False)
        self.assertIsNone(item.dedupe_key)
        self.assertIsNone(item.dedupe_expires_at)

    # ---- 恢复校验：非法元数据统一 ValueError 且保持原状态 ----
    def assert_restore_rejected(self, bad):
        self.cache.push('d', 'e', 100)
        before = self.cache.snapshot()
        calls_before = self.clock_calls[0]
        with self.assertRaises(ValueError):
            self.cache.restore(bad)
        self.assertEqual(self.clock_calls[0], calls_before)
        self.assertEqual(self.cache.snapshot(), before)

    def test_restore_rejects_bad_metadata_container_and_length(self):
        base = {'values': {}, 'events': ['a'], 'seen': {}, 'max_queue': None}
        good = [{'dedupe_known': True, 'dedupe_key': 'k',
                 'dedupe_expires_at': 10}]
        self.assert_restore_rejected(dict(base, event_metadata='x'))
        self.assert_restore_rejected(dict(base, event_metadata=None))
        self.assert_restore_rejected(dict(base, event_metadata=[]))
        self.assert_restore_rejected(dict(base, event_metadata=good * 2))

    def test_restore_rejects_bad_metadata_entry_shape(self):
        base = {'values': {}, 'events': ['a'], 'seen': {}, 'max_queue': None}
        self.assert_restore_rejected(dict(base, event_metadata=['x']))
        self.assert_restore_rejected(dict(base, event_metadata=[{}]))
        self.assert_restore_rejected(dict(base, event_metadata=[
            {'dedupe_known': True, 'dedupe_key': 'k'}]))
        self.assert_restore_rejected(dict(base, event_metadata=[
            {'dedupe_known': True, 'dedupe_key': 'k',
             'dedupe_expires_at': 1, 'extra': 2}]))

    def test_restore_rejects_non_bool_known_flag(self):
        base = {'values': {}, 'events': ['a'], 'seen': {}, 'max_queue': None}
        for bad_known in (0, 1, 'true', None):
            self.assert_restore_rejected(dict(base, event_metadata=[
                {'dedupe_known': bad_known, 'dedupe_key': 'k',
                 'dedupe_expires_at': 1}]))

    def test_restore_rejects_bad_metadata_key_and_time(self):
        base = {'values': {}, 'events': ['a'], 'seen': {}, 'max_queue': None}
        for bad_key in ([], {}, set()):
            self.assert_restore_rejected(dict(base, event_metadata=[
                {'dedupe_known': True, 'dedupe_key': bad_key,
                 'dedupe_expires_at': 1}]))
        for bad_time in (float('nan'), float('inf'), 'x', True, []):
            self.assert_restore_rejected(dict(base, event_metadata=[
                {'dedupe_known': True, 'dedupe_key': 'k',
                 'dedupe_expires_at': bad_time}]))

    def test_restore_accepts_none_key_and_none_expiry_when_known(self):
        # None 是真实的去重键；到期点为 None 同样合法
        self.cache.restore({
            'values': {}, 'events': ['a'], 'seen': {}, 'max_queue': None,
            'event_metadata': [{'dedupe_known': True, 'dedupe_key': None,
                                'dedupe_expires_at': None}]})
        item = self.cache.peek_with_metadata()[0]
        self.assertIs(item.dedupe_known, True)
        self.assertIsNone(item.dedupe_key)


class ReleaseDedupeTest(unittest.TestCase):
    def setUp(self):
        self.now = [100]
        self.clock_calls = [0]

        def clock():
            self.clock_calls[0] += 1
            return self.now[0]

        self.clock = clock
        self.cache = EventCache(clock)

    def advance(self, seconds):
        self.now[0] += seconds

    # ---- 单项入口：基本返回形状 ----
    def test_release_existing_returns_released(self):
        self.cache.push('d', 'e1', 10)
        result = self.cache.release_dedupe('d')
        self.assertEqual(set(result), {'released', 'reason'})
        self.assertIs(result.released, True)
        self.assertIsNone(result.reason)
        self.assertNotIn('d', self.cache.seen)

    def test_release_missing_returns_missing(self):
        result = self.cache.release_dedupe('never-seen')
        self.assertEqual(set(result), {'released', 'reason'})
        self.assertIs(result.released, False)
        self.assertEqual(result.reason, 'missing')
        self.assertEqual(self.cache.seen, {})

    def test_repeated_release_reports_true_then_missing(self):
        self.cache.push('d', 'e1', 10)
        first = self.cache.release_dedupe('d')
        second = self.cache.release_dedupe('d')
        self.assertIs(first.released, True)
        self.assertIs(second.released, False)
        self.assertEqual(second.reason, 'missing')

    def test_none_is_a_real_dedupe_key(self):
        self.assertTrue(self.cache.push(None, 'e1', 10))
        released = self.cache.release_dedupe(None)
        missing = self.cache.release_dedupe(None)
        self.assertIs(released.released, True)
        self.assertIs(missing.released, False)
        self.assertEqual(missing.reason, 'missing')

    # ---- 不读取时钟；到期点是否已过都删除 ----
    def test_release_never_reads_clock(self):
        self.cache.push('d', 'e1', 10)
        before = self.clock_calls[0]
        self.cache.release_dedupe('d')
        self.cache.release_dedupe('d')  # missing 同样不读时钟
        self.cache.release_dedupe('other')
        self.assertEqual(self.clock_calls[0], before)

    def test_release_removes_record_even_after_expiry_point_passed(self):
        self.cache.push('d', 'e1', 10)  # 到期点 110
        self.advance(100)
        # 不做任何清理：惰性过期记录仍在 seen 中，释放照样删除并报 released
        self.assertIn('d', self.cache.seen)
        result = self.cache.release_dedupe('d')
        self.assertIs(result.released, True)
        self.assertNotIn('d', self.cache.seen)

    def test_release_after_cleanup_reports_missing(self):
        self.cache.push('d', 'e1', 10)
        self.advance(11)
        self.cache.cleanup()  # 已按既有语义清掉 seen 记录
        result = self.cache.release_dedupe('d')
        self.assertIs(result.released, False)
        self.assertEqual(result.reason, 'missing')

    def test_release_does_not_advance_reject_regression_watermark(self):
        cache = EventCache(self.clock, clock_policy='reject_regression')
        cache.push('d', 'e1', 10)  # 首次采样，水位 100
        self.assertEqual(cache.clock_status().last_time, 100)
        cache.release_dedupe('d')
        cache.release_dedupe('missing')
        # 释放不采样时钟：水位保持不变
        self.assertEqual(cache.clock_status().last_time, 100)

    # ---- 只触碰 seen ----
    def test_release_does_not_touch_values_events_receipts_or_history(self):
        self.cache.put('k', 'v', 1000)
        pushed = self.cache.push_with_receipt('d', 'e1', 1000)
        receipt = pushed.receipt
        before = (
            dict(self.cache.values),
            list(self.cache.events),
            list(self.cache.event_expiries),
            list(self.cache.event_receipts),
            self.cache._next_receipt,
            self.cache.discard_history(),
            self.cache.queue_status(),
        )
        result = self.cache.release_dedupe('d')
        self.assertIs(result.released, True)
        after = (
            dict(self.cache.values),
            list(self.cache.events),
            list(self.cache.event_expiries),
            list(self.cache.event_receipts),
            self.cache._next_receipt,
            self.cache.discard_history(),
            self.cache.queue_status(),
        )
        self.assertEqual(before, after)
        # 旧事件仍在 FIFO 中、回执仍可取消；释放不写丢弃审计
        self.assertEqual(self.cache.cancel(receipt),
                         Result(removed=True, event='e1', reason=None))
        self.assertEqual(self.cache.discard_history(), [])

    def test_release_keeps_old_event_metadata_unchanged(self):
        self.cache.push('d', 'e1', 10)  # 到期点 110
        self.cache.release_dedupe('d')
        [item] = self.cache.peek_with_metadata()
        self.assertEqual(item.event, 'e1')
        self.assertIs(item.dedupe_known, True)
        self.assertEqual(item.dedupe_key, 'd')
        self.assertEqual(item.dedupe_expires_at, 110)  # 入队时刻的到期点不变

    def test_release_allows_immediate_repush_and_both_events_coexist(self):
        self.assertTrue(self.cache.push('d', 'e1', 1000))
        # 未释放时窗口内被拦截
        self.assertFalse(self.cache.push('d', 'blocked', 1000))
        self.cache.release_dedupe('d')
        repush = self.cache.push_with_reason('d', 'e2', 1000)
        self.assertTrue(repush.accepted)
        self.assertIsNone(repush.reason)
        # 新旧事件按 FIFO 同时存在，去重占用按新 push 重新登记
        self.assertEqual(self.cache.pop_batch(), ['e1', 'e2'])
        self.assertEqual(self.cache.seen['d'], 1100)

    def test_release_does_not_write_discard_history_on_eviction_setup(self):
        cache = EventCache(self.clock, max_queue=1,
                           overflow_policy='drop_oldest',
                           discard_history_limit=10)
        cache.push('a', 'e1', 1000)
        cache.push('b', 'e2', 1000)  # 挤出 e1，历史一条 queue_full
        before = cache.discard_history()
        self.assertEqual(len(before), 1)
        cache.release_dedupe('a')
        cache.release_dedupe('b')
        cache.release_dedupe('missing')
        self.assertEqual(cache.discard_history(), before)

    def test_release_after_pop_still_frees_dedupe(self):
        # pop 只释放队列槽位、保留 seen；释放负责结束窗口
        self.assertTrue(self.cache.push('d', 'e1', 1000))
        self.assertEqual(self.cache.pop(), 'e1')
        self.assertIn('d', self.cache.seen)
        self.assertFalse(self.cache.push('d', 'e2', 1000))
        self.assertIs(self.cache.release_dedupe('d').released, True)
        self.assertTrue(self.cache.push('d', 'e3', 1000))

    # ---- 不可哈希键 ----
    def test_unhashable_dedupe_raises_type_error_without_clock_or_state(self):
        self.cache.push('d', 'e1', 10)
        snapshot_before = self.cache.snapshot()
        for bad in (['x'], {'y': 1}, {1, 2}):
            calls_before = self.clock_calls[0]
            with self.assertRaises(TypeError):
                self.cache.release_dedupe(bad)
            self.assertEqual(self.clock_calls[0], calls_before)
        # seen、队列、回执、历史全部不变
        self.assertEqual(self.cache.snapshot(), snapshot_before)

    # ---- inspect 等只读视图：除 seen 外结果不变 ----
    def test_release_only_changes_dedupe_view_in_inspect(self):
        self.cache.put('k', 'v', 1000)
        self.cache.push('d', 'e1', 1000)
        before = self.cache.inspect()
        self.cache.release_dedupe('d')
        after = self.cache.inspect()
        self.assertEqual(before.value_count, after.value_count)
        self.assertEqual(before.expired_value_count, after.expired_value_count)
        self.assertEqual(before.queue_size, after.queue_size)
        self.assertEqual(before.live_event_count, after.live_event_count)
        self.assertEqual(before.expired_event_count, after.expired_event_count)
        self.assertEqual(before.next_value_expiry, after.next_value_expiry)
        self.assertEqual(before.next_event_expiry, after.next_event_expiry)
        self.assertEqual(before.discard_history_size, after.discard_history_size)
        # 仅去重占用少了一条
        self.assertEqual(before.dedupe_count - 1, after.dedupe_count)
        self.assertEqual(self.cache.peek(), ['e1'])
        self.assertEqual(self.cache.queue_status(), Result(size=1, max_queue=None))

    # ---- apply_batch ----
    def test_apply_batch_release_then_push_accepted_in_order(self):
        self.cache.push('d', 'e1', 1000)
        results = self.cache.apply_batch([
            ('release_dedupe', 'd'),
            ('push', 'd', 'e2', 1000),
        ])
        self.assertEqual([set(r) for r in results],
                         [{'released', 'reason'}, {'accepted', 'reason'}])
        self.assertIs(results[0].released, True)
        self.assertTrue(results[1].accepted)
        self.assertEqual(self.cache.pop_batch(), ['e1', 'e2'])

    def test_apply_batch_double_release_true_then_missing(self):
        self.cache.push('d', 'e1', 10)
        results = self.cache.apply_batch([
            ('release_dedupe', 'd'),
            ('release_dedupe', 'd'),
        ])
        self.assertIs(results[0].released, True)
        self.assertIs(results[1].released, False)
        self.assertEqual(results[1].reason, 'missing')

    def test_apply_batch_push_release_push_same_key_in_one_batch(self):
        # 同刻 push 先登记占用，释放后同键可再次入队，两条事件共存
        results = self.cache.apply_batch([
            ('push', 'd', 'e1', 1000),
            ('release_dedupe', 'd'),
            ('push', 'd', 'e2', 1000),
        ])
        self.assertTrue(results[0].accepted)
        self.assertIs(results[1].released, True)
        self.assertTrue(results[2].accepted)
        self.assertEqual(self.cache.pop_batch(), ['e1', 'e2'])

    def test_apply_batch_release_follows_single_batch_clock_convention(self):
        # 与 delete-only 批次一致：非空批次整批只读一次时钟；释放本身不另读
        before = self.clock_calls[0]
        self.cache.apply_batch([('release_dedupe', 'd'),
                                ('release_dedupe', 'd')])
        self.assertEqual(self.clock_calls[0] - before, 1)

    def test_apply_batch_release_does_not_touch_queue(self):
        self.cache.push_with_receipt('d', 'e1', 1000)
        results = self.cache.apply_batch([('release_dedupe', 'd')])
        self.assertIs(results[0].released, True)
        self.assertEqual(self.cache.peek_with_receipt(),
                         [Result(event='e1', receipt=1)])
        self.assertEqual(self.cache._next_receipt, 2)

    def test_apply_batch_invalid_release_rejects_whole_batch_before_clock(self):
        self.cache.put('k', 'v', 100)
        self.cache.push_with_receipt('d0', 'e0', 100)
        before = self.cache.snapshot()
        for bad, exc in (
            ([('release_dedupe',)], ValueError),                    # 缺键
            ([('release_dedupe', 'd', 'x')], ValueError),           # 多余元素
            ([('release_dedupe', ['x'])], TypeError),               # 不可哈希
            ([('release_dedupe', 'd0'), ('push', 'd', 'e', -1)], ValueError),
            ([('put', 'a', 1, 10), ('release_dedupe', ['x'])], TypeError),
        ):
            calls_before = self.clock_calls[0]
            with self.assertRaises(exc):
                self.cache.apply_batch(bad)
            self.assertEqual(self.clock_calls[0], calls_before)  # 预校验在前
        # 缓存、回执、历史与时钟水位全部保持整批执行前原样
        self.assertEqual(self.cache.snapshot(), before)
        self.assertEqual(self.cache.clock_status().last_time, None)

    # ---- replay_batch ----
    def test_replay_release_never_reads_injected_clock(self):
        results = self.cache.replay_batch([
            (0, ('push', 'd', 'e1', 100)),
            (50, ('release_dedupe', 'd')),
            (50, ('release_dedupe', 'd')),
        ])
        self.assertEqual(self.clock_calls[0], 0)
        self.assertIs(results[1].released, True)
        self.assertIs(results[2].released, False)
        self.assertEqual(results[2].reason, 'missing')

    def test_replay_release_then_push_in_later_record(self):
        results = self.cache.replay_batch([
            (0, ('push', 'd', 'e1', 100)),    # 占用至 100
            (20, ('release_dedupe', 'd')),
            (30, ('push', 'd', 'e2', 100)),   # 已释放：接受
        ])
        self.assertIs(results[1].released, True)
        self.assertTrue(results[2].accepted)
        self.assertEqual(self.cache.pop_batch(), ['e1', 'e2'])
        self.assertEqual(self.cache.seen['d'], 130)

    def test_replay_release_reports_released_even_past_record_time(self):
        # 回放不随记录时间推进触发清理：t=100 时记录（到期点 10）仍在
        results = self.cache.replay_batch([
            (0, ('push', 'd', 'e1', 10)),
            (100, ('release_dedupe', 'd')),
            (101, ('release_dedupe', 'd')),
        ])
        self.assertIs(results[1].released, True)
        self.assertEqual(results[2].reason, 'missing')

    def test_replay_invalid_release_record_rejects_whole_batch(self):
        self.cache.push('d0', 'e0', 100)
        before = self.cache.snapshot()
        for records, exc in (
            ([(0, ('release_dedupe',))], ValueError),
            ([(0, ('release_dedupe', 'd', 1))], ValueError),
            ([(0, ('release_dedupe', ['x']))], TypeError),
            ([(10, ('release_dedupe', 'd')), (5, ('push', 'd', 'e', 1))],
             ValueError),  # 时间戳倒退
        ):
            with self.assertRaises(exc):
                self.cache.replay_batch(records)
        self.assertEqual(self.cache.snapshot(), before)

    # ---- 快照与恢复 ----
    def test_snapshot_seen_reflects_release_and_roundtrip(self):
        self.cache.push('d', 'e1', 1000)
        self.cache.push('g', 'e2', 1000)
        self.cache.release_dedupe('d')
        snap = self.cache.snapshot()
        self.assertNotIn('d', snap.seen)
        self.assertIn('g', snap.seen)

        restored = EventCache(self.clock)
        restored.restore(snap)
        # 恢复后的 seen 准确反映释放结果：d 无占用可立即 push，g 仍在窗口内
        self.assertEqual(restored.release_dedupe('d').reason, 'missing')
        self.assertIs(restored.release_dedupe('g').released, True)

    def test_old_snapshot_without_new_fields_still_restores(self):
        # 不增加任何必填字段：旧四字段快照恢复后释放照常工作
        old_snapshot = {
            'values': {},
            'events': ['old-event'],
            'seen': {'d': 123},
            'max_queue': None,
        }
        cache = EventCache(self.clock)
        self.assertIsNone(cache.restore(old_snapshot))
        self.assertIs(cache.release_dedupe('d').released, True)
        self.assertEqual(cache.release_dedupe('d').reason, 'missing')
        # 旧事件（未知键）不被释放触碰
        self.assertEqual(cache.pop(), 'old-event')


class RenewEventTest(unittest.TestCase):
    def setUp(self):
        self.now = [100]
        self.clock_calls = [0]

        def clock():
            self.clock_calls[0] += 1
            return self.now[0]

        self.clock = clock
        self.cache = EventCache(clock)

    def advance(self, seconds):
        self.now[0] += seconds

    def make(self, max_queue=None, overflow_policy='reject_new',
             discard_history_limit=None, clock_policy='allow_regression'):
        return EventCache(self.clock, max_queue=max_queue,
                          overflow_policy=overflow_policy,
                          discard_history_limit=discard_history_limit,
                          clock_policy=clock_policy)

    def assertRenewed(self, result, event, expires_at):
        self.assertIsInstance(result, Result)
        self.assertEqual(set(result), {'renewed', 'event', 'reason', 'expires_at'})
        self.assertIs(result.renewed, True)
        self.assertIs(result['renewed'], True)
        self.assertIs(result.event, event)
        self.assertIsNone(result.reason)
        self.assertEqual(result['expires_at'], expires_at)

    def assertMissing(self, result):
        self.assertEqual(result, Result(
            renewed=False, event=None, reason='missing', expires_at=None))

    def assertExpired(self, result, event):
        self.assertIsInstance(result, Result)
        self.assertEqual(set(result), {'renewed', 'event', 'reason', 'expires_at'})
        self.assertIs(result.renewed, False)
        self.assertIs(result.event, event)
        self.assertEqual(result.reason, 'expired')
        self.assertIsNone(result.expires_at)

    # ---- 续期成功：返回绝对到期点 ----
    def test_renew_live_event_sets_absolute_expiry(self):
        r = self.cache.push_expiring_with_receipt('d', 'e', 1000, 50)  # 到期点 150
        out = self.cache.renew_event(r.receipt, 20)
        self.assertRenewed(out, 'e', 120)
        self.assertEqual(list(self.cache.event_expiries), [120])
        self.assertEqual(self.clock_calls[0], 2)  # push 与 renew 各读一次

    def test_renew_returns_observation_time_plus_ttl(self):
        r = self.cache.push_expiring_with_receipt('d', 'e', 1000, 1000)
        self.advance(30)
        out = self.cache.renew_event(r.receipt, 7)
        self.assertRenewed(out, 'e', 137)  # 观察时刻 130 + 7

    def test_renew_event_without_ttl_gets_expiry(self):
        # 原先无 event_ttl 的事件（expiry 为 None）续期后获得绝对到期点
        r = self.cache.push_with_receipt('d', 'plain', 1000)
        self.assertEqual(list(self.cache.event_expiries), [None])
        out = self.cache.renew_event(r.receipt, 25)
        self.assertRenewed(out, 'plain', 125)
        self.assertEqual(list(self.cache.event_expiries), [125])

    def test_renew_can_shorten_as_well_as_extend(self):
        r = self.cache.push_expiring_with_receipt('d', 'e', 1000, 1000)
        out = self.cache.renew_event(r.receipt, 1)  # ttl 缩短同样合法
        self.assertRenewed(out, 'e', 101)

    def test_zero_ttl_accepted_then_boundary_renew_is_expired(self):
        r = self.cache.push_expiring_with_receipt('d', 'e', 1000, 100)
        out = self.cache.renew_event(r.receipt, 0)  # 新到期点恰为 100
        self.assertRenewed(out, 'e', 100)
        # 同刻再次续期：到期点 <= 观察时刻，遵循既有边界按 expired 处理
        self.assertExpired(self.cache.renew_event(r.receipt, 5), 'e')

    def test_repeated_renew_chains_expiry(self):
        r = self.cache.push_expiring_with_receipt('d', 'e', 1000, 10)  # 110
        self.cache.renew_event(r.receipt, 20)  # 120
        self.advance(15)                      # 115，仍存活
        out = self.cache.renew_event(r.receipt, 10)  # 125
        self.assertRenewed(out, 'e', 125)
        self.advance(10)                      # 125：边界过期
        self.assertExpired(self.cache.renew_event(r.receipt, 1), 'e')

    def test_none_event_is_returned_as_is(self):
        r = self.cache.push_expiring_with_receipt('d', None, 1000, 100)
        out = self.cache.renew_event(r.receipt, 10)
        self.assertIsNone(out.event)
        self.assertIs(out.renewed, True)
        self.assertEqual(list(self.cache.events), [None])

    def test_reads_clock_exactly_once_on_hit(self):
        r = self.cache.push_expiring_with_receipt('d', 'e', 1000, 1000)
        self.clock_calls[0] = 0
        self.cache.renew_event(r.receipt, 10)
        self.assertEqual(self.clock_calls[0], 1)
        self.advance(1000)
        self.cache.renew_event(r.receipt, 10)  # 过期路径同样只读一次
        self.assertEqual(self.clock_calls[0], 2)

    # ---- 续期只改到期点：位置、内容、回执、去重、容量、values 全不变 ----
    def test_renew_keeps_position_content_receipt_metadata_and_capacity(self):
        cache = self.make(max_queue=5, overflow_policy='drop_oldest')
        cache.push_expiring_with_receipt('d1', 'a', 1000, 100)
        target = cache.push_expiring_with_receipt('d2', 'b', 1000, 100)
        cache.push_with_receipt('d3', 'c', 1000)
        before = (list(cache.events), list(cache.event_receipts),
                  [(m.dedupe_known, m.dedupe_key, m.dedupe_expires_at)
                   for m in cache.event_metadata], dict(cache.seen),
                  cache.queue_status().size, cache.max_queue, cache.overflow_policy,
                  dict(cache.values), cache._next_receipt,
                  [dict(h) for h in cache.discard_history()])
        out = cache.renew_event(target.receipt, 900)
        self.assertRenewed(out, 'b', 1000)
        after = (list(cache.events), list(cache.event_receipts),
                 [(m.dedupe_known, m.dedupe_key, m.dedupe_expires_at)
                  for m in cache.event_metadata], dict(cache.seen),
                 cache.queue_status().size, cache.max_queue, cache.overflow_policy,
                 dict(cache.values), cache._next_receipt,
                 [dict(h) for h in cache.discard_history()])
        self.assertEqual(before, after)  # 除 event_expiries 外逐项不变
        self.assertEqual(list(cache.event_expiries), [200, 1000, None])

    def test_renew_does_not_touch_values_or_seen_window(self):
        self.cache.put('k', 'v', 100)
        r = self.cache.push_expiring_with_receipt('d', 'e', 2, 50)  # seen 到 102
        self.advance(50)
        self.cache.renew_event(r.receipt, 100)
        self.assertEqual(self.cache.values['k'], ('v', 200))  # values 不被触碰
        self.assertEqual(self.cache.seen['d'], 102)           # 去重窗口不延长

    def test_renewed_expiry_is_visible_to_inspect(self):
        r = self.cache.push_expiring_with_receipt('d', 'e', 1000, 10)  # 110
        self.cache.renew_event(r.receipt, 100)  # 200
        snap = self.cache.inspect()
        self.assertEqual(snap.queue_size, 1)
        self.assertEqual(snap.live_event_count, 1)
        self.assertEqual(snap.next_event_expiry, 200)

    def test_event_without_ttl_never_expires_via_renew(self):
        # 无 event_ttl 的事件无论时钟推进多远，续期总是成功并赋新到期点
        r = self.cache.push_with_receipt('d', 'e', 1000)
        self.advance(100000)
        out = self.cache.renew_event(r.receipt, 5)
        self.assertRenewed(out, 'e', 100105)

    # ---- 已到期：边界、移除、历史、去重保留、容量释放、回执不复用 ----
    def test_expired_at_boundary_removes_event_and_returns_original(self):
        r = self.cache.push_expiring_with_receipt('d', 'e', 1000, 50)  # 150
        self.advance(50)
        out = self.cache.renew_event(r.receipt, 10)  # 150 <= 150
        self.assertExpired(out, 'e')
        self.assertEqual(list(self.cache.events), [])
        self.assertEqual(list(self.cache.event_expiries), [])
        self.assertEqual(list(self.cache.event_receipts), [])
        self.assertEqual(list(self.cache.event_metadata), [])

    def test_expired_middle_keeps_other_events_fifo_and_alignment(self):
        self.cache.push_expiring_with_receipt('d1', 'a', 1000, 100)
        middle = self.cache.push_expiring_with_receipt('d2', 'b', 1000, 10)
        self.cache.push_with_receipt('d3', 'c', 1000)
        self.advance(50)
        self.assertExpired(self.cache.renew_event(middle.receipt, 10), 'b')
        self.assertEqual(list(self.cache.events), ['a', 'c'])
        self.assertEqual(list(self.cache.event_expiries), [200, None])
        self.assertEqual(list(self.cache.event_receipts), [1, 3])
        self.assertEqual([m.dedupe_key for m in self.cache.event_metadata],
                         ['d1', 'd3'])

    def test_expired_writes_event_ttl_discard_history_at_observation_time(self):
        r = self.cache.push_expiring_with_receipt('d', None, 1000, 40)  # 140
        self.advance(60)
        self.assertExpired(self.cache.renew_event(r.receipt, 10), None)
        history = self.cache.discard_history()
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0],
                         Result(event=None, reason='event_ttl', timestamp=160))

    def test_expired_keeps_dedupe_window(self):
        cache = self.make(max_queue=1)
        r = cache.push_expiring_with_receipt('d', 'e', 100, 10)  # seen 到 110
        self.advance(10)
        self.assertExpired(cache.renew_event(r.receipt, 10), 'e')
        # 去重窗口保留到原截止时刻：同键仍被拦截，槽位却已释放给他键
        self.assertEqual(cache.push_with_reason('d', 'again', 100).reason,
                         'dedupe_window')
        self.assertIn('d', cache.seen)
        self.assertTrue(cache.push('other', 'new', 100))

    def test_expired_receipt_not_reusable_for_cancel_or_renew(self):
        r = self.cache.push_expiring_with_receipt('d', 'e', 1000, 0)
        self.assertExpired(self.cache.renew_event(r.receipt, 10), 'e')
        self.clock_calls[0] = 0
        self.assertMissing(self.cache.renew_event(r.receipt, 10))
        self.assertEqual(self.cache.cancel(r.receipt),
                         Result(removed=False, event=None, reason='missing'))
        self.assertEqual(self.clock_calls[0], 0)  # missing 路径不读时钟
        # 新事件继续分配新回执，旧号不复用
        self.assertEqual(self.cache.push_with_receipt('d2', 'f', 10).receipt, 2)

    def test_expired_frees_slot_for_later_push_in_same_batch(self):
        cache = self.make(max_queue=1)
        results = cache.apply_batch([
            ('push_expiring_with_receipt', 'd', 'e', 100, 0),  # 到期点即批次时刻
            ('renew_event', 1, 10),                            # 到期出队
            ('push_with_receipt', 'd2', 'f', 100),             # 槽位已释放
        ])
        self.assertExpired(results[1], 'e')
        self.assertIs(results[2].accepted, True)
        self.assertEqual(cache.pop(), 'f')

    # ---- 普通出队不新增 TTL 判断 ----
    def test_plain_pop_still_ignores_renewed_expiry(self):
        r = self.cache.push_expiring_with_receipt('d', 'e', 1000, 100)
        self.cache.push_with_receipt('d2', 'f', 1000)
        self.cache.renew_event(r.receipt, 0)  # 到期点已到，但普通出队不判 TTL
        self.advance(999)
        self.assertEqual(self.cache.pop(), 'e')          # FIFO 原样取出
        self.assertEqual(self.cache.pop_with_receipt().event, 'f')
        self.assertEqual(self.cache.pop_batch(), [])

    # ---- missing：不读时钟 ----
    def test_unknown_receipt_returns_missing_without_clock(self):
        self.assertMissing(self.cache.renew_event(999, 10))
        self.assertEqual(self.clock_calls[0], 0)

    def test_popped_cancelled_evicted_receipts_return_missing(self):
        first = self.cache.push_with_receipt('d1', 'a', 10)
        second = self.cache.push_with_receipt('d2', 'b', 10)
        third = self.cache.push_with_receipt('d3', 'c', 10)
        self.cache.pop_with_receipt()                  # 1 已出队
        self.cache.cancel(second.receipt)              # 2 已取消
        self.assertEqual(self.cache.renew_event(first.receipt, 10).reason,
                         'missing')
        self.assertEqual(self.cache.renew_event(second.receipt, 10).reason,
                         'missing')
        # 未知号与现存号对照
        self.assertMissing(self.cache.renew_event(999, 10))
        self.assertIs(self.cache.renew_event(third.receipt, 10).renewed, True)
        self.assertEqual(self.clock_calls[0], 4)  # 三次 push + 一次命中续期

    def test_missing_does_not_change_any_state(self):
        before = self.cache.snapshot()
        self.cache.renew_event(123, 10)
        self.assertEqual(self.cache.snapshot(), before)

    # ---- 校验：receipt / ttl 非法时 ValueError，不读时钟、不改状态 ----
    def test_invalid_receipt_raises_value_error_atomically(self):
        r = self.cache.push_expiring_with_receipt('d', 'e', 1000, 100)
        for bad in (0, -1, True, False, 1.5, 2.0, '1', None, 1j, [], {}):
            calls_before = self.clock_calls[0]
            with self.assertRaises(ValueError):
                self.cache.renew_event(bad, 10)
            self.assertEqual(self.clock_calls[0], calls_before)
        self.assertEqual(list(self.cache.event_expiries), [200])  # 到期点不变
        self.assertIs(self.cache.renew_event(r.receipt, 10).renewed, True)

    def test_invalid_ttl_raises_value_error_atomically(self):
        r = self.cache.push_expiring_with_receipt('d', 'e', 1000, 100)
        for bad in (-1, float('nan'), float('inf'), -float('inf'),
                    '10', None, True, False, 1j, [], {}):
            calls_before = self.clock_calls[0]
            with self.assertRaises(ValueError):
                self.cache.renew_event(r.receipt, bad)
            self.assertEqual(self.clock_calls[0], calls_before)
        # 即使回执未知，非法 ttl 仍先抛 ValueError，且不读时钟
        self.clock_calls[0] = 0
        with self.assertRaises(ValueError):
            self.cache.renew_event(999, -1)
        self.assertEqual(self.clock_calls[0], 0)
        self.assertEqual(list(self.cache.event_expiries), [200])  # 到期点不变

    def test_clock_exception_propagates_without_state_change(self):
        class Boom(Exception):
            pass

        def bad_clock():
            raise Boom()

        cache = EventCache(bad_clock)
        cache.restore({
            'values': {}, 'events': ['e'], 'seen': {}, 'max_queue': None,
            'event_expiries': [500], 'event_receipts': [1], 'next_receipt': 2,
            'event_metadata': [{
                'dedupe_known': True, 'dedupe_key': 'd',
                'dedupe_expires_at': 200}],
        })
        with self.assertRaises(Boom):
            cache.renew_event(1, 10)
        self.assertEqual(list(cache.event_expiries), [500])  # 到期点不更新
        self.assertEqual(list(cache.events), ['e'])
        # missing 路径不接触坏时钟
        self.assertEqual(cache.renew_event(2, 10).reason, 'missing')

    def test_clock_regression_rejection_preserves_state(self):
        now_box = [10]
        cache = EventCache(lambda: now_box[0],
                           clock_policy='reject_regression')
        r = cache.push_expiring_with_receipt('d', 'e', 100, 100)  # 到期 110
        now_box[0] = 5
        with self.assertRaises(app.ClockRegressionError):
            cache.renew_event(r.receipt, 100)
        self.assertEqual(list(cache.event_expiries), [110])  # 到期点不更新
        self.assertEqual(cache.clock_status().last_time, 10)  # 水位不回退
        now_box[0] = 10
        out = cache.renew_event(r.receipt, 5)  # 水位恢复后续期成功
        self.assertRenewed(out, 'e', 15)

    # ---- apply_batch：整批预校验、单次时钟、按序生效、结果对齐 ----
    def test_apply_batch_renew_event_shapes_and_alignment(self):
        results = self.cache.apply_batch([
            ('push_expiring_with_receipt', 'd1', 'e1', 1000, 50),  # 1 到期150
            ('push_with_receipt', 'd2', 'e2', 1000),               # 2 无 TTL
            ('renew_event', 1, 10),
            ('renew_event', 2, 7),
            ('renew_event', 99, 5),
        ])
        self.assertRenewed(results[2], 'e1', 110)
        self.assertRenewed(results[3], 'e2', 107)
        self.assertMissing(results[4])
        self.assertEqual(self.clock_calls[0], 1)  # 非空批次整批只读一次
        self.assertEqual(list(self.cache.events), ['e1', 'e2'])
        self.assertEqual(list(self.cache.event_receipts), [1, 2])

    def test_apply_batch_renew_then_later_ops_see_effect_in_order(self):
        results = self.cache.apply_batch([
            ('push_expiring_with_receipt', 'd', 'e', 100, 0),  # 到期点即此刻
            ('renew_event', 1, 5),                             # expired，出队
            ('inspect',),
        ])
        self.assertExpired(results[1], 'e')
        self.assertEqual(results[2].queue_size, 0)
        self.assertEqual(results[2].discard_history_size, 1)

    def test_apply_batch_renew_validation_rejects_whole_batch_before_clock(self):
        self.cache.push_expiring_with_receipt('d', 'e', 1000, 100)
        before = self.cache.snapshot()
        for bad in (
            [('renew_event', 0, 5)],
            [('renew_event', 1, -1)],
            [('renew_event', True, 5)],
            [('renew_event', 1, True)],
            [('renew_event', 1)],
            [('renew_event', 1, 2, 3)],
            [('renew_event', 1, 5), ('bogus',)],
        ):
            calls_before = self.clock_calls[0]
            with self.assertRaises(ValueError):
                self.cache.apply_batch(bad)
            self.assertEqual(self.clock_calls[0], calls_before)
        self.assertEqual(self.cache.snapshot(), before)

    # ---- replay_batch：只用记录时间、不读注入时钟 ----
    def test_replay_renew_uses_record_timestamp_and_never_reads_clock(self):
        results = self.cache.replay_batch([
            (10, ('push_expiring_with_receipt', 'd', 'e', 1000, 100)),  # 110
            (50, ('renew_event', 1, 30)),   # 80
            (70, ('renew_event', 1, 5)),    # 75
            (75, ('renew_event', 1, 1)),    # 边界相等 -> expired
            (75, ('renew_event', 1, 1)),    # missing
        ])
        self.assertEqual(self.clock_calls[0], 0)  # 全程不读注入时钟
        self.assertRenewed(results[1], 'e', 80)
        self.assertRenewed(results[2], 'e', 75)
        self.assertExpired(results[3], 'e')
        self.assertMissing(results[4])
        history = self.cache.discard_history()
        self.assertEqual(history, [Result(event='e', reason='event_ttl',
                                          timestamp=75)])
        self.assertEqual(list(self.cache.events), [])

    def test_replay_renew_plain_event_and_determinism(self):
        records = [
            (0, ('push_with_receipt', 'd', 'plain', 1000)),
            (100, ('renew_event', 1, 25)),
        ]
        first = self.make()
        second = self.make()
        r1 = first.replay_batch(records)
        r2 = second.replay_batch(records)  # 同构初始状态重放结果一致
        self.assertEqual([dict(x) for x in r1], [dict(x) for x in r2])
        self.assertRenewed(r1[1], 'plain', 125)
        self.assertEqual(list(first.event_expiries), [125])

    def test_replay_renew_frees_slot_for_later_record(self):
        results = self.cache.replay_batch([
            (0, ('resize_queue', 1)),
            (0, ('push_expiring_with_receipt', 'd', 'e', 100, 0)),
            (10, ('renew_event', 1, 1)),                    # 已到期出队
            (10, ('push_with_receipt', 'd2', 'f', 100)),   # 槽位已释放
        ])
        self.assertExpired(results[2], 'e')
        self.assertIs(results[3].accepted, True)
        self.assertEqual(list(self.cache.events), ['f'])

    def test_replay_invalid_renew_record_rolls_back_whole_batch(self):
        before = self.cache.snapshot()
        for bad in (
            [(0, ('push_with_receipt', 'd', 'e', 10)), (5, ('renew_event', 0, 5))],
            [(0, ('renew_event', 1, -5))],
            [(0, ('renew_event', True, 5))],
            [(0, ('renew_event', 1, 5, 9))],
        ):
            with self.assertRaises(ValueError):
                self.cache.replay_batch(bad)
        self.assertEqual(self.cache.snapshot(), before)

    # ---- 快照：续期后的到期点、回执、元数据关系完整保留 ----
    def test_renewed_expiries_survive_snapshot_restore(self):
        self.cache.replay_batch([
            (0, ('push_with_receipt', 'd1', 'plain', 1000)),
            (0, ('push_expiring_with_receipt', 'd2', 'ttl', 1000, 50)),
            (10, ('renew_event', 1, 5)),    # 原无 TTL -> 15
            (10, ('renew_event', 2, 90)),   # 60 -> 100
        ])
        snap = self.cache.snapshot()
        self.assertEqual(snap.event_expiries, [15, 100])
        self.assertEqual(snap.event_receipts, [1, 2])
        restored = self.make()
        self.assertIsNone(restored.restore(snap))
        self.assertEqual(list(restored.events), ['plain', 'ttl'])
        self.assertEqual(list(restored.event_expiries), [15, 100])
        # 恢复后由当前时钟按既有 <= 边界判定：时刻 100 两者皆到期
        now_box = [100]
        target = EventCache(lambda: now_box[0])
        target.restore(snap)
        out = target.discard_expired_events()
        self.assertEqual(out.events_removed, 2)
        self.assertEqual([x.event for x in out.discarded], ['plain', 'ttl'])

    def test_restore_old_snapshot_treats_event_as_no_ttl_and_renew_works(self):
        # 缺 event_expiries 的旧快照：事件按无 TTL 解释，续期照常赋到期点
        cache = self.make()
        cache.restore({
            'values': {}, 'events': ['old'], 'seen': {}, 'max_queue': None,
            'event_receipts': [1], 'next_receipt': 2,
        })
        self.assertEqual(list(cache.event_expiries), [None])
        out = cache.renew_event(1, 10)  # 观察时刻 100
        self.assertRenewed(out, 'old', 110)
        self.assertEqual(cache.snapshot().event_expiries, [110])


if __name__ == '__main__':
    unittest.main()
