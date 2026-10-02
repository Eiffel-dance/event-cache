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
    def test_snapshot_has_exactly_four_fields_and_result_access(self):
        snap = self.seed().snapshot()
        self.assertIsInstance(snap, Result)
        self.assertEqual(set(snap), {'values', 'events', 'seen', 'max_queue'})
        self.assertEqual(snap.max_queue, 3)
        self.assertEqual(snap['max_queue'], 3)
        self.assertEqual(snap.events, ['e1', 'e2'])
        self.assertEqual(snap['events'], ['e1', 'e2'])
        self.assertEqual(snap.seen, {'d1': 110, 'd2': 200})
        self.assertEqual(snap['seen'], {'d1': 110, 'd2': 200})
        # values 与 dict.values 方法同名，属性访问仍须取到条目
        self.assertEqual(snap.values, {'k1': ('v1', 150), 'k2': ('v2', 300)})
        self.assertEqual(snap['values'], snap.values)

    def test_snapshot_events_is_plain_list(self):
        snap = self.seed().snapshot()
        self.assertIsInstance(snap.events, list)

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
    def test_snapshot_without_ttl_events_keeps_four_fields(self):
        self.cache.push('d', 'e', 10)
        snap = self.cache.snapshot()
        self.assertEqual(set(snap), {'values', 'events', 'seen', 'max_queue'})

    def test_snapshot_with_ttl_events_adds_aligned_expiries(self):
        self.cache.push('d1', 'e1', 10)
        self.cache.push_expiring('d2', 'e2', 10, 50)
        self.cache.push_expiring('d3', 'e3', 10, 5)
        snap = self.cache.snapshot()
        self.assertEqual(set(snap), {'values', 'events', 'seen', 'max_queue', 'event_expiries'})
        self.assertEqual(snap.events, ['e1', 'e2', 'e3'])
        self.assertEqual(snap['event_expiries'], [None, 150, 105])
        self.assertEqual(snap.event_expiries, [None, 150, 105])  # 属性访问可用
        self.assertIsInstance(snap.event_expiries, list)

    def test_snapshot_expiries_detached_from_cache(self):
        self.cache.push_expiring('d', 'e', 10, 50)
        snap = self.cache.snapshot()
        snap['event_expiries'][0] = 999
        self.assertEqual(list(self.cache.event_expiries), [150])

    def test_snapshot_after_cleanup_of_all_ttl_events_has_four_fields(self):
        self.cache.push_expiring('d', 'e', 10, 0)
        self.cache.push('d2', 'plain', 10)
        self.cache.cleanup_expired_events()
        snap = self.cache.snapshot()
        self.assertEqual(set(snap), {'values', 'events', 'seen', 'max_queue'})

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
        # 恢复后快照回到四字段格式
        self.assertEqual(set(self.cache.snapshot()), {'values', 'events', 'seen', 'max_queue'})

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


if __name__ == '__main__':
    unittest.main()
