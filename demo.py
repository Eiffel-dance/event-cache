from app import EventCache
now=[100]; c=EventCache(lambda:now[0]); c.put('status','ready',30); c.push('event-1',{'type':'ready'},10); print(c.get('status'),c.pop())
