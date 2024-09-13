import pytest
import time
import concurrent.futures
import os
import glob
# from versioned_kvstore.versioned_kvstore import TimeMap
from datetime import datetime, timezone


from versioned_kvstore.versioned_kvstore_lsm import TimeMapLSM
# def test_should_pass():
#     a = 1
#     b = 1
#     assert a == b


def _get_utc_ms():
    return int(datetime.now(timezone.utc).timestamp()*1000000)

def _clean_data_files():
    for f in glob.glob(os.path.join(_get_directory(), 'sstable_*.pkl')):
        os.remove(f)

def _get_directory():
    return './versioned_kvstore/data-test2/'

def _clean_directory():
    os.rmdir(_get_directory())

def _create_directory(directory):
    os.makedirs(_get_directory() if not directory else directory, exist_ok=True)


def test_no_such_key():
    timeMap = TimeMapLSM(directory=_get_directory(), memtable_capacity = 2, lru_capacity = 2)
    timeMap.put('k1', 'v1')
    timeMap.put('k2', 'v2')
    time_stamp = _get_utc_ms()
    value = timeMap.get('k3', time_stamp)
    assert value == ''


def test_too_small_timestamp():
    timeMap = TimeMapLSM(directory=_get_directory(), memtable_capacity = 2, lru_capacity = 2)
    timeMap.put('k1', 'v1')
    timeMap.put('k2', 'v2')
    time_stamp = 100
    value = timeMap.get('k1', time_stamp)
    assert value == ''

def test_large_timestamp():
    timeMap = TimeMapLSM(directory=_get_directory(), memtable_capacity = 2, lru_capacity = 2)
    timeMap.put('k1', 'v1')
    timeMap.put('k2', 'v2')
    time_stamp = _get_utc_ms()+1000000000000
    value = timeMap.get('k1', time_stamp)
    assert value == 'v1'


def test_concurrent_write():
    timeMap = TimeMapLSM(directory=_get_directory(), memtable_capacity = 2, lru_capacity = 2)


    def put(key, value):
        timeMap.put(key, value)
    def get(key, timeStamp):
        return timeMap.get(key, timeStamp)

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        # executor.map(thread_function, range(3))
        futures = []
        futures.append(executor.submit(put, 'k1', 'v1'))
        futures.append(executor.submit(get, 'k1', _get_utc_ms()+10000))
        futures.append(executor.submit(put, 'k1', 'v2'))
        futures.append(executor.submit(put, 'k1', 'v3'))
        res = [f.result() for f in futures]

    assert res[1] == 'v1'

    time_stamp = _get_utc_ms()+1000000000000

    value = timeMap.get('k1', time_stamp)
    
    assert value == 'v3'



def test_concurrent_time():
    _clean_data_files()
    timeMap = TimeMapLSM(directory=_get_directory(), memtable_capacity = 2, lru_capacity = 2)


    def put(key, value, time):
        timeMap.put_internal(key, value, time)
    def get(key, timeStamp):
        return timeMap.get(key, timeStamp)

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        # executor.map(thread_function, range(3))
        futures = []
        timestamp = _get_utc_ms()
        futures.append(executor.submit(put, 'k1', 'v1', timestamp))
        futures.append(executor.submit(get, 'k1', timestamp))
        futures.append(executor.submit(put, 'k1', 'v2', timestamp))
        futures.append(executor.submit(put, 'k1', 'v3', timestamp))
        futures.append(executor.submit(put, 'k2', 'v4', timestamp))
        futures.append(executor.submit(put, 'k2', 'v5', timestamp))
        futures.append(executor.submit(put, 'k2', 'v6', timestamp))
        res = [f.result() for f in futures]

    assert res[1] == 'v1'

    time_stamp = timestamp

    value = timeMap.get('k1', time_stamp)
    
    assert value == 'v3'
    _clean_data_files()



if __name__ == '__main__':
    pytest.main()
    _clean_data_files()


