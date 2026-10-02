# Event Cache

A dependency-free Python reference implementation for cache, queue, message-system.

Run with: python3 demo.py
Tests: python3 -m unittest discover -s tests -v

## Scope

实现一个带过期时间和去重键的内存事件缓存。写入、读取、删除和过期清理必须有清晰的时间语义；相同去重键在窗口内只能保留一条事件，队列按插入顺序出队并能报告丢弃原因。实现不依赖后台线程，时间源可注入，便于确定性回放和故障测试。

## 队列事件有效期

- `push_expiring(dedupe, event, window, event_ttl)` /
  `push_expiring_with_reason(...)`：在去重窗口之外为事件本体设置独立 TTL，
  以接受时刻（注入时钟只读取一次）加 TTL 得到到期时刻，TTL=0 表示接受时刻
  即已到期。TTL 必须是有限非负数，否则抛 `ValueError` 且不读时钟、不改状态；
  去重键不可哈希抛 `TypeError`。去重判断仍先于容量判断，因 `dedupe_window`
  或 `queue_full` 被拒时不入队、不创建或延长去重占用。
- 到期事件在显式清理前继续占用容量、保持 FIFO；`pop`/`pop_batch` 不读取时钟，
  时间流逝不会自动移除事件。
- `cleanup_expired_events()`：单次读取时钟，移除全部到期事件并返回
  `events_removed`，保持未到期事件顺序；不触碰 `values`、`seen` 和不设事件
  TTL 的旧事件。
- `push_batch` 条目可为三元组，或在末尾追加 `event_ttl` 的四元组；
  `apply_batch` 支持 `('push_expiring', dedupe, event, window, event_ttl)`。
  两者先完整校验输入，再读取一次时钟，任何校验错误都不会改变状态。
- 快照不含带 TTL 事件时仍是 `values/events/seen/max_queue` 四字段；包含时
  增加与 `events` 等长对齐的 `event_expiries`（旧事件为 `None`）。`restore`
  同时接受新旧格式，字段长度不一致或到期信息非法时抛 `ValueError` 并保留
  原状态。
