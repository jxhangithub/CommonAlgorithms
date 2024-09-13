import pytest
import unittest
import time
import concurrent.futures
import os
import glob
# from versioned_kvstore.versioned_kvstore import TimeMap
from datetime import datetime, timezone

# pip install -U pytest
from lsm import TimeMapLSM


class TestLSM(unittest.TestCase):
  def test_should_pass(self):
      a = 1
      b = 1
      assert a == b

  def _get_directory(self):
    return './data-test2/'

  def _get_utc_ms(self):
      return int(datetime.now(timezone.utc).timestamp()*1000000)

  def _clean_data_files(self):
      for f in glob.glob(os.path.join(self._get_directory(), 'sstable_*.pkl')):
          os.remove(f)

  def _clean_directory(self):
      os.rmdir(self._get_directory())

  def _create_directory(self, directory):
      os.makedirs(self._get_directory() if not directory else directory, exist_ok=True)



  def test_no_such_key(self):
      timeMap = TimeMapLSM(directory=self._get_directory(), memtable_capacity = 2, lru_capacity = 2)
      timeMap.put('k1', 'v1')
      timeMap.put('k2', 'v2')
      time_stamp = self._get_utc_ms()
      value = timeMap.get('k3', time_stamp)
      assert value == ''


  def test_too_small_timestamp(self):
      timeMap = TimeMapLSM(directory=self._get_directory(), memtable_capacity = 2, lru_capacity = 2)
      timeMap.put('k1', 'v1')
      timeMap.put('k2', 'v2')
      time_stamp = 100
      value = timeMap.get('k1', time_stamp)
      assert value == ''

  def test_large_timestamp(self):
      timeMap = TimeMapLSM(directory=self._get_directory(), memtable_capacity = 2, lru_capacity = 2)
      timeMap.put('k1', 'v1')
      timeMap.put('k2', 'v2')
      time_stamp = self._get_utc_ms()+1000000000000
      value = timeMap.get('k1', time_stamp)
      assert value == 'v1'


  def test_concurrent_write(self):
      timeMap = TimeMapLSM(directory=self._get_directory(), memtable_capacity = 2, lru_capacity = 2)


      def put(key, value):
          timeMap.put(key, value)
      def get(key, timeStamp):
          return timeMap.get(key, timeStamp)

      with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
          # executor.map(thread_function, range(3))
          futures = []
          futures.append(executor.submit(put, 'k1', 'v1'))
          futures.append(executor.submit(get, 'k1', self._get_utc_ms()+10000))
          futures.append(executor.submit(put, 'k1', 'v2'))
          futures.append(executor.submit(put, 'k1', 'v3'))
          res = [f.result() for f in futures]

      assert res[1] == 'v1'

      time_stamp = self._get_utc_ms()+1000000000000

      value = timeMap.get('k1', time_stamp)
      
      assert value == 'v3'



  def test_concurrent_time(self):
      self._clean_data_files()
      timeMap = TimeMapLSM(directory=self._get_directory(), memtable_capacity = 2, lru_capacity = 2)


      def put(key, value, time):
          timeMap.put_internal(key, value, time)
      def get(key, timeStamp):
          return timeMap.get(key, timeStamp)

      with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
          # executor.map(thread_function, range(3))
          futures = []
          timestamp = self._get_utc_ms()
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
      # self._clean_data_files()



if __name__ == '__main__':
    unittest.main()
    # _clean_data_files()


