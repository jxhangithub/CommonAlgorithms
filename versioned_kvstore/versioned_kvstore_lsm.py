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


1. concurrent read /write: deque, server
2. out of memeory: file
3. future version
4. index

"""

# import time
import collections, bisect
from datetime import datetime, timezone
# import threading
# from readerwriterlock import rwlock
import os
import pickle

class MemTable:

    def __init__(self, start_time: int = 0, data = None):
        self.data = collections.defaultdict(collections.deque) if not data else data
        self.start_time = int(datetime.now(timezone.utc).timestamp()*1000000) if start_time == 0 else start_time
        

    def set_internal(self, key: str, value: str, timestamp: int = None) -> None:
        timestamp = timestamp if timestamp != None else int(datetime.now(timezone.utc).timestamp()*1000000)
        self.data[key].append((timestamp, value))



    # def _update(self) -> None:
    #     while self.update:
    #         try:
    #             k,v,t = self.update.pop()
    #         except:
    #             print('empty deque')
    #             break
    #         self.data[k].append((t, v))


    def set(self, key: str, value: str) -> None:
        timestamp = int(datetime.now(timezone.utc).timestamp()*1000000)
        self.set_internal(key, value, timestamp)

    def get(self, key: str, timestamp: int) -> str:
        
        if key not in self.data:
            return ''
        
        idx = bisect.bisect_left(list(self.data[key]), (timestamp + 1, ''))
        
        if idx == 0:
            return ''
        
        return self.data[key][idx - 1][1]
    
    def flush(self, filename):
        with open(filename, 'wb') as f:
            pickle.dump(self.data, f)
    

# if data are large, need to load most frequent data in memory, push large data to file
# key, most recent key, 
# time down sampling, 

# multiple machine -> sharding, 


# future time, return latest, wait??
# index ??

# 1. how to handle concurent issue,   ANS:read+write lock. 
# 2. server bottle neck , if server resources limited, the data uploaded in Memery, but the dataset is too big.  ANS: keep most recently hitted data section in Memory, and preload most fr‍‍‌‍‍‌‍‌‌‍‍‍‌‌‍‍‌‌‍equent datasets in Memory.
# 3. future time.




# import os
# import pickle

# class MemTable:
#     def __init__(self):
#         self.data = {}

#     def get(self, key):
#         return self.data.get(key)

#     def put(self, key, value):
#         self.data[key] = value

#     def flush(self, filename):
#         with open(filename, 'wb') as f:
#             pickle.dump(self.data, f)

class TimeMapLSM: # ToDo: add bloom filter
    def __init__(self, directory):
        self.directory = directory
        self.memtable = MemTable()
        self.sstables = dict()

    def get(self, key, timestamp):
        if timestamp >= self.memtable.start_time and key in self.memtable.data:
            return self.memtable.get(key, timestamp)

        for sstable_file in self.sstables:
            with open(sstable_file, 'rb') as f:
                data = pickle.load(f)
                file_table = MemTable(time, data)
                if key in data:
                    return file_table.get(key, timestamp)

        return None

    def put(self, key, value):
        self.memtable.put(key, value)

        if len(self.memtable.data) > 10:
            self.flush_memtable()

    def flush_memtable(self):
        filename = os.path.join(self.directory, f'sstable_{len(self.sstables)}.pkl')
        self.memtable.flush(filename)
        self.sstables[self.memtable.start_time] = filename
        self.memtable = MemTable()

# Usage
# tree = LSMTree('data')
# tree.put('key1', 'value1')
# tree.put('key2', 'value2')
# print(tree.get('key1')) 