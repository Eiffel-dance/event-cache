# Event Cache

A dependency-free Python reference implementation for cache, queue, message-system.

Run with: python3 demo.py
Tests: python3 -m unittest discover -s tests -v

## Scope

实现一个带过期时间和去重键的内存事件缓存。写入、读取、删除和过期清理必须有清晰的时间语义；相同去重键在窗口内只能保留一条事件，队列按插入顺序出队并能报告丢弃原因。实现不依赖后台线程，时间源可注入，便于确定性回放和故障测试。

## 队列溢出策略

有限队列（`max_queue` 非 None）已满时的行为由构造参数 `overflow_policy` 决定：

- `'reject_new'`（默认，省略时同此）：拒绝新事件，`push_with_reason` 等返回 `accepted=False, reason='queue_full'`，不移除事件，也不登记或延长去重窗口。
- `'drop_oldest'`：去重检查通过且 `max_queue > 0` 时移除 FIFO 队首并接受新事件；被挤出事件同步移除其 `event_ttl` 元数据，原 dedupe 记录保留到窗口截止。去重窗口内的请求优先返回 `dedupe_window`，不会为腾位挤出事件；容量为零时仍拒绝且不丢弃。

`drop_oldest` 下 `push_with_reason`、`push_expiring_with_reason`、`push_batch`、`apply_batch` 与 `replay_batch` 的入队结果额外包含 `discarded` 列表（未挤出为空，挤出一项时为 `Result(event=原事件, reason='queue_full')`，None 事件同样保留）；`push` / `push_expiring` 仍只返回布尔值。非默认策略的快照保存 `overflow_policy` 字段，`restore` 可恢复；旧四/五字段快照按 `'reject_new'` 解释。

