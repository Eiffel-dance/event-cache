from app import EventCache

now = [100]
c = EventCache(lambda: now[0])

# 缓存：ttl 以写入时刻 + ttl 记录到期点，边界为到期点 <= 当前时刻
c.put('status', 'ready', 30)
c.put('temp', 'warm', 5)
c.push('event-1', {'type': 'ready'}, 10)
print('get status:', c.get('status'))
print('delete missing:', c.delete('nope'))

now[0] += 6  # 手动推进时钟
print('get temp (已过期，返回 None 并删除):', c.get('temp'))
print('delete temp (已不存在):', c.delete('temp'))

# 去重窗口：同键窗口内被拒绝，带 reason 的结果报告
print('push event-1 再次:', c.push('event-1', {'type': 'dup'}, 10))
print('push_with_reason:', c.push_with_reason('event-1', {'type': 'dup'}, 10))

now[0] += 5  # 超过去重窗口 (10 + 6 = 16 到期，当前 111)
print('cleanup:', c.cleanup())
print('push event-1 窗口后:', c.push_with_reason('event-1', {'type': 'again'}, 10))

# FIFO 出队，空队列返回 None
print('pop:', c.pop(), '| pop:', c.pop(), '| pop:', c.pop())
