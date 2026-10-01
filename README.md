# Event Cache

A dependency-free Python reference implementation for cache, queue, message-system.

Run with: python3 demo.py
Tests: python3 -m unittest discover -s tests -v

## Scope

实现一个带过期时间和去重键的内存事件缓存。写入、读取、删除和过期清理必须有清晰的时间语义；相同去重键在窗口内只能保留一条事件，队列按插入顺序出队并能报告丢弃原因。实现不依赖后台线程，时间源可注入，便于确定性回放和故障测试。
