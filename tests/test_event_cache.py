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


if __name__ == '__main__':
    unittest.main()
