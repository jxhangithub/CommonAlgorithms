import pytest
import time
import concurrent.futures

# from versioned_kvstore.versioned_kvstore import TimeMap
from datetime import datetime, timezone


from versioned_kvstore.versioned_kvstore_lsm import TimeMapLSM
# def test_should_pass():
#     a = 1
#     b = 1
#     assert a == b




def test_no_such_key():
    timeMap = TimeMapLSM()
    timeMap.set('k1', 'v1')
    timeMap.set('k2', 'v2')
    time_stamp = int(datetime.now(timezone.utc).timestamp()*1000000)
    value = timeMap.get('k3', time_stamp)
    assert value == ''


def test_too_small_timestamp():
    timeMap = TimeMapLSM()
    timeMap.set('k1', 'v1')
    timeMap.set('k2', 'v2')
    time_stamp = 100
    value = timeMap.get('k1', time_stamp)
    assert value == ''

def test_large_timestamp():
    timeMap = TimeMapLSM()
    timeMap.set('k1', 'v1')
    timeMap.set('k2', 'v2')
    time_stamp = int(datetime.now(timezone.utc).timestamp()*1000000+1000000000000)
    value = timeMap.get('k1', time_stamp)
    assert value == 'v1'


def test_concurrent_write():
    timeMap = TimeMapLSM()


    def set(key, value):
        timeMap.set(key, value)
    def get(key, timeStamp):
        return timeMap.get(key, timeStamp)

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        # executor.map(thread_function, range(3))
        futures = []
        futures.append(executor.submit(set, 'k1', 'v1'))
        futures.append(executor.submit(get, 'k1', int(datetime.now(timezone.utc).timestamp()*1000000)+10000))
        futures.append(executor.submit(set, 'k1', 'v2'))
        futures.append(executor.submit(set, 'k1', 'v3'))
        res = [f.result() for f in futures]

    assert res[1] == 'v1'

    time_stamp = int(datetime.now(timezone.utc).timestamp()*1000000+1000000000000)

    value = timeMap.get('k1', time_stamp)
    
    assert value == 'v3'



def test_concurrent_time():
    timeMap = TimeMapLSM()


    def set(key, value, time):
        timeMap.set_internal(key, value, time)
    def get(key, timeStamp):
        return timeMap.get(key, timeStamp)

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        # executor.map(thread_function, range(3))
        futures = []
        futures.append(executor.submit(set, 'k1', 'v1', 100))
        futures.append(executor.submit(get, 'k1', 100))
        futures.append(executor.submit(set, 'k1', 'v2', 100))
        futures.append(executor.submit(set, 'k1', 'v3', 100))
        res = [f.result() for f in futures]

    assert res[1] == 'v1'

    time_stamp = 100

    value = timeMap.get('k1', time_stamp)
    
    assert value == 'v3'



if __name__ == '__main__':
    pytest.main()


