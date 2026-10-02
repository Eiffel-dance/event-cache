import unittest
import app
from app import EventCache


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
        self.calls = []
        self.cache = EventCache(self._clock)

    def _clock(self):
        self.calls.append(self.now[0])
        return self.now[0]

    def advance(self, seconds):
        self.now[0] += seconds

    # ---- 基础顺序与结果语义 ----
    def test_returns_results_in_input_order(self):
        results = self.cache.push_batch([
            ('d1', 'e1', 10),
            ('d2', 'e2', 10),
            ('d1', 'e1-dup', 10),
        ])
        self.assertEqual([r.accepted for r in results], [True, True, False])
        self.assertEqual([r.reason for r in results], [None, None, 'dedupe_window'])
        self.assertEqual([self.cache.pop() for _ in range(2)], ['e1', 'e2'])
        self.assertIsNone(self.cache.pop())

    def test_reason_semantics_match_push_with_reason(self):
        results = self.cache.push_batch([('d', 'e', 10)])
        single = self.cache.push_with_reason('d', 'e2', 10)
        self.assertEqual(results[0].accepted, True)
        self.assertEqual(results[0].reason, None)
        self.assertEqual(results[0]['accepted'], True)
        self.assertEqual(single.accepted, False)
        self.assertEqual(single.reason, 'dedupe_window')

    def test_accepts_generators_and_other_iterables(self):
        results = self.cache.push_batch(('d%d' % i, 'e%d' % i, 10) for i in range(3))
        self.assertEqual([r.accepted for r in results], [True, True, True])
        self.assertEqual([self.cache.pop() for _ in range(3)], ['e0', 'e1', 'e2'])

    # ---- 单一时钟时刻 ----
    def test_clock_read_once_for_whole_batch(self):
        results = self.cache.push_batch([
            ('d1', 'e1', 10),
            ('d2', 'e2', 10),
            ('d3', 'e3', 10),
        ])
        self.assertEqual(len(self.calls), 1)
        self.assertTrue(all(r.accepted for r in results))

    def test_batch_uses_one_moment_even_across_rejections(self):
        self.cache.push('d0', 'e0', 10)
        self.calls.clear()
        results = self.cache.push_batch([
            ('d1', 'e1', 10),
            ('d1', 'e1-dup', 10),  # 同批次前项刚登记，窗口内拒绝
            ('d2', 'e2', 10),
        ])
        self.assertEqual(len(self.calls), 1)
        self.assertEqual([r.reason for r in results], [None, 'dedupe_window', None])

    def test_window_zero_boundary_within_batch(self):
        # window=0 => 到期点 == 批次时刻，<= 边界下同批次可重复入队
        results = self.cache.push_batch([
            ('d', 'e1', 0),
            ('d', 'e2', 0),
            ('d', 'e3', 0),
        ])
        self.assertTrue(all(r.accepted for r in results))
        self.assertEqual([self.cache.pop() for _ in range(3)], ['e1', 'e2', 'e3'])

    # ---- 容量与拒绝副作用 ----
    def test_queue_full_after_capacity_consumed_within_batch(self):
        cache = EventCache(lambda: self.now[0], max_queue=2)
        results = cache.push_batch([
            ('d1', 'e1', 10),
            ('d2', 'e2', 10),
            ('d3', 'e3', 10),
        ])
        self.assertEqual([r.reason for r in results], [None, None, 'queue_full'])
        self.assertEqual([cache.pop(), cache.pop()], ['e1', 'e2'])
        self.assertNotIn('d3', cache.seen)  # 被拒项不登记去重

    def test_dedupe_window_priority_over_queue_full_in_batch(self):
        cache = EventCache(lambda: self.now[0], max_queue=1)
        cache.push('d1', 'e0', 100)
        results = cache.push_batch([
            ('d2', 'e2', 10),   # 队列已满
            ('d1', 'e1-dup', 100),  # 去重窗口优先
        ])
        self.assertEqual([r.reason for r in results], ['queue_full', 'dedupe_window'])
        self.assertNotIn('d2', cache.seen)
        self.assertEqual(cache.seen['d1'], self.now[0] + 100)  # 旧占用未延长

    def test_rejected_item_keeps_fifo_insertion_order(self):
        self.cache.push_batch([
            ('d1', 'e1', 10),
            ('d1', 'rejected', 10),
            ('d2', 'e2', 10),
        ])
        self.assertEqual([self.cache.pop(), self.cache.pop()], ['e1', 'e2'])

    # ---- 空批次 ----
    def test_empty_batch_returns_empty_without_clock(self):
        self.assertEqual(self.cache.push_batch([]), [])
        self.assertEqual(self.calls, [])

    def test_empty_generator_does_not_read_clock(self):
        self.assertEqual(self.cache.push_batch(iter(())), [])
        self.assertEqual(self.calls, [])

    # ---- 校验与原子性 ----
    def test_non_iterable_batch_raises_value_error(self):
        for bad in (None, 42, 3.5, object()):
            with self.assertRaises(ValueError):
                self.cache.push_batch(bad)

    def test_malformed_items_raise_value_error(self):
        for bad_items in (
            [('d1', 'e1')],                    # 缺 window
            [('d1', 'e1', 10, 'extra')],       # 多出成员
            [('d1',)],                         # 只给 dedupe
            [42],                              # 条目不可解析
            [None],
            ['d1'],                            # 字符串展开后不足三元
            [('d1', 'e1', 10), ('d2', 'e2')],  # 后项缺成员
        ):
            with self.assertRaises(ValueError):
                self.cache.push_batch(bad_items)

    def test_invalid_window_raises_value_error(self):
        for bad in (-1, float('nan'), float('inf'), -float('inf'), True, '10', None):
            with self.assertRaises(ValueError):
                self.cache.push_batch([('d1', 'e1', 10), ('d2', 'e2', bad)])

    def test_validation_failure_changes_nothing(self):
        self.cache.put('k', 'v', 100)
        self.cache.push('pre', 'pre-event', 100)
        values_before = dict(self.cache.values)
        seen_before = dict(self.cache.seen)
        events_before = list(self.cache.events)
        self.calls.clear()
        for bad_items in (
            None,
            [('d1', 'e1', 10), ('d2', 'e2', -1)],
            [('d1', 'e1', 10), ('d2', 'e2')],
        ):
            with self.assertRaises(ValueError):
                self.cache.push_batch(bad_items)
        self.assertEqual(self.calls, [])  # 校验失败不读取时钟
        self.assertEqual(self.cache.values, values_before)
        self.assertEqual(self.cache.seen, seen_before)
        self.assertEqual(list(self.cache.events), events_before)

    def test_late_invalid_item_means_no_partial_results(self):
        # 前两项本可接受，但第三项非法：整批失败且无部分写入
        try:
            self.cache.push_batch([
                ('d1', 'e1', 10),
                ('d2', 'e2', 10),
                ('d3', 'e3', 'bad'),
            ])
        except ValueError:
            pass
        else:
            self.fail('ValueError expected')
        self.assertEqual(list(self.cache.events), [])
        self.assertEqual(self.cache.seen, {})
        self.assertIsNone(self.cache.pop())

    # ---- 不隐式 cleanup / 不改变生命周期 ----
    def test_batch_does_not_implicitly_cleanup(self):
        self.cache.push('old', 'old-event', 5)
        self.advance(10)  # old 的去重记录已到期但未清理
        results = self.cache.push_batch([('new', 'new-event', 10)])
        self.assertTrue(results[0].accepted)
        self.assertIn('old', self.cache.seen)  # 未触发清理
        self.advance(100)
        # 已排队事件生命周期不受时间推进影响
        self.assertEqual(
            [self.cache.pop(), self.cache.pop()], ['old-event', 'new-event']
        )

    def test_single_entrypoints_behaviour_unchanged(self):
        self.assertTrue(self.cache.push('d', 'e', 10))
        self.assertFalse(self.cache.push('d', 'e2', 10))
        r = self.cache.push_with_reason('d', 'e3', 10)
        self.assertFalse(r.accepted)
        self.assertEqual(r.reason, 'dedupe_window')
        self.assertEqual(self.cache.queue_status().size, 1)
        self.assertEqual(self.cache.pop(), 'e')
        self.assertIsNone(self.cache.pop())


if __name__ == '__main__':
    unittest.main()
