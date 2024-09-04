import pytest
import time

from versioned_kvstore.versioned_kvstore import TimeMap
from datetime import datetime, timezone

# def test_should_pass():
#     a = 1
#     b = 1
#     assert a == b




def test_no_such_key():
    timeMap = TimeMap()
    timeMap.set('k1', 'v1')
    timeMap.set('k2', 'v2')
    time_stamp = int(datetime.now(timezone.utc).timestamp()*1000000)
    value = timeMap.get('k3', time_stamp)
    assert value == ''


def test_too_small_timestamp():
    timeMap = TimeMap()
    timeMap.set('k1', 'v1')
    timeMap.set('k2', 'v2')
    time_stamp = 100
    value = timeMap.get('k1', time_stamp)
    assert value == ''

def test_large_key():
    timeMap = TimeMap()
    timeMap.set('k1', 'v1')
    timeMap.set('k2', 'v2')
    time_stamp = int(datetime.now(timezone.utc).timestamp()*1000000)
    value = timeMap.get('k1', time_stamp)
    assert value == 'v1'





if __name__ == '__main__':
    pytest.main()


