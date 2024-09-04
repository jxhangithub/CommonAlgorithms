"""
Algorithms and Data Structures Interview: In this interview, 
you will design and code a data structure to accurately store time-based information.

[LC 久八一，原题没有变种，]
(https://leetcode.com/problems/time-based-key-value-store/description/?envType=company&envId=openai&favoriteSlug=openai-all)


自己产生一个 timestamp


1. testing case ,
following up: 
1. how to handle concurent issue,   ANS:read+write lock. 
2. server bottle neck , if server resources limited, the data uploaded in Memery, but the dataset is too big.  
    ANS: keep most recently hitted data section in Memory, and preload most fr‍‍‌‍‍‌‍‌‌‍‍‍‌‌‍‍‌‌‍equent datasets in Memory.
3. future time.


然后follow-up问说如果多线程同时读写该如何optimize。我给write加了个thread lock，又是现查现写。问read需不需要lock我思考了一下感觉不需要


1. KV store 里面，每个 key 存一个 sorted list，list 是一个 (timestamp, value) 的 tuple，然后 sorted by timestamp;
  在 query 的时候，用一个 bisect right 来找到最大的 timestamp match to query
2. 针对多线程的 follow up，这里可能需要的就是 paging 的思想，把 key space 进行 paging 分别上锁从而能够增加并发程度
3. 针对 OOM 的 follow up，一种方法是利用 LRU 等一些方法把不太常用的 key 写到磁盘里面去节省空间。简单的方法就是直接 append 到一个 file 里面去，
  然后需要查找的时候就线性查找; 如果需要更加优化的方法的话，可能要考虑实现简单版本的 SST



1. Make update consistency even in multithreading。
我的理解因为多线程的情况下会产生一起update一个key with same timestamp，
所以这个情况就是需要加锁保证同一时间只有一个update或者用thread safe的data structure。
2. when invoke get, pass in a future timestamp. my answer was to add a sleep in the get implementation,
 wait for the time to reach the input timestamp and proceed.  
我的理解是可以define system rule来处理这个情况，譬如说return null，或者说return latest value。

其实这题还有一问呢。要建 index, 我只有时间口述了下inverted index。


1. concurrent read /write
2. out of memeory
3. future version
4. index

"""

import time
import collections, bisect
from datetime import datetime, timezone
class TimeMap:

    def __init__(self):
        self.data = collections.defaultdict(list)

    def set(self, key: str, value: str) -> None:

        # self.data[key].append((time.time_ns(), value))
        self.data[key].append((int(datetime.now(timezone.utc).timestamp()*1000000), value))


    def get(self, key: str, timestamp: int) -> str:
        if key not in self.data:
            return ''
        
        idx = bisect.bisect_left(self.data[key], (timestamp + 1, ''))
        
        if idx == 0:
            return ''
        
        return self.data[key][idx - 1][1]
    
