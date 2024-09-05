import threading

class ReadWriteLock:
    def __init__(self):
        self.readers = 0
        self.lock = threading.Lock()
        self.read_condition = threading.Condition(self.lock)
        self.write_condition = threading.Condition(self.lock)

    def acquire_read(self):
        with self.lock:
            while self.readers < 0:
                self.read_condition.wait()  # Wait if a writer is present
            self.readers += 1

    def release_read(self):
        with self.lock:
            self.readers -= 1
            if self.readers == 0:
                self.write_condition.notify()  # Notify waiting writers

    def acquire_write(self):
        with self.lock:
            while self.readers > 0:
                self.write_condition.wait()  # Wait if readers are present
            self.readers = -1  # Indicate that a writer has acquired the lock

    def release_write(self):
        with self.lock:
            self.readers = 0
            self.write_condition.notify_all()  # Notify waiting readers and writers

# Example usage
def reader(lock, reader_id):
    lock.acquire_read()
    print(f"Reader {reader_id} is reading.")
    lock.release_read()

def writer(lock, writer_id):
    lock.acquire_write()
    print(f"Writer {writer_id} is writing.")
    lock.release_write()

# Create the lock
rw_lock = ReadWriteLock()

# Create threads for readers and writers
threads = []
for i in range(5):
    threads.append(threading.Thread(target=reader, args=(rw_lock, i)))
for i in range(2):
    threads.append(threading.Thread(target=writer, args=(rw_lock, i)))

# Start threads
for thread in threads:
    thread.start()

# Wait for all threads to finish
for thread in threads:
    thread.join()