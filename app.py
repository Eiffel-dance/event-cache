from collections import deque
class EventCache:
    def __init__(self,clock): self.clock=clock; self.values={}; self.events=deque(); self.seen={}
    def put(self,key,value,ttl): self.values[key]=(value,self.clock()+ttl)
    def get(self,key):
        item=self.values.get(key)
        if not item: return None
        if item[1]<=self.clock(): self.values.pop(key,None); return None
        return item[0]
    def push(self,dedupe,event,window):
        now=self.clock(); self.seen={k:t for k,t in self.seen.items() if t>now}
        if dedupe in self.seen: return False
        self.seen[dedupe]=now+window; self.events.append(event); return True
    def pop(self): return self.events.popleft() if self.events else None
