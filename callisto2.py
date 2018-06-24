import os
import sqlite3
import time
from threading import Event, Lock, Thread


class CommitterThread(Thread):
    def __init__(self, callisto):
        super(CommitterThread, self).__init__(daemon=True)
        self.callisto = callisto
        self.stop_event = Event()

    def run(self):
        while not self.stop_event.is_set():
            time.sleep(self.callisto.commit_interval)
            self.callisto.commit()


class CallistoDatabase:
    def __init__(self, db):
        self.db = db
        self.cursor = db.cursor()
        self.uncommitted_changes = 0
        self.commit_lock = Lock()

    def prime(self):
        self.db.execute('CREATE TABLE m(rid INTEGER PRIMARY KEY ASC, content TEXT)')

    def put(self, message):
        with self.commit_lock:
            self.uncommitted_changes += 1
            self.cursor.execute('INSERT INTO m (content) VALUES (?)', [message])

    def count(self):
        c = self.db.cursor()
        c.execute('SELECT COUNT(rid) FROM m')
        return c.fetchone()[0]

    def get(self, start=0, end=-1):
        if end < 0:
            end = self.count() + 1 + end
        if start < 0:
            start = self.count() + 1 + start

        start = max(0, start)
        end = max(0, end)
        c = self.db.cursor()
        c.execute('SELECT content FROM m WHERE rid BETWEEN ? and ? ORDER BY rid', [start, end])
        while True:
            chunk = c.fetchmany(128)
            if not chunk:
                break
            for row in chunk:
                yield row[0]

    def commit(self):
        with self.commit_lock:
            if self.uncommitted_changes:
                self.cursor.execute('COMMIT')
                self.uncommitted_changes = 0

    def close(self):
        self.commit()
        self.cursor = None
        self.db.close()
        self.db = None


class Callisto:
    def __init__(self, base_path, commit_interval=1):
        self.base_path = base_path
        self.db_cache = {}
        self.commit_interval = commit_interval
        self.committer_thread = CommitterThread(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def put(self, list_id, message):
        return self.get_db(list_id).put(message)

    def get(self, list_id, start=0, end=-1):
        return self.get_db(list_id).get(start=start, end=end)

    def count(self, list_id):
        return self.get_db(list_id).count()

    def commit(self):
        for db in self.db_cache.values():
            db.commit()

    def close(self):
        if self.committer_thread.is_alive():
            self.committer_thread.stop_event.set()
            self.committer_thread.join()
        db_cache = self.db_cache
        self.db_cache = None
        for db in db_cache.values():
            db.close()

    def get_db(self, list_id):
        if self.db_cache is None:
            raise RuntimeError('Callisto closing')
        if list_id in self.db_cache:
            return self.db_cache[list_id]
        db_filename = self.get_db_filename(list_id)
        is_new = False
        if not os.path.isfile(db_filename):
            db_dir = os.path.dirname(db_filename)
            if not os.path.isdir(db_dir):
                os.makedirs(db_dir)
            is_new = True

        sqlite_db = sqlite3.connect(db_filename, check_same_thread=False)
        sqlite_db.row_factory = sqlite3.Row
        db = self.db_cache[list_id] = CallistoDatabase(sqlite_db)
        if is_new:
            db.prime()
        return db

    def get_db_filename(self, list_id):
        return os.path.join(self.base_path, list_id[:5], f'{list_id}.db')

    def delete(self, list_id):
        self.db_cache.pop(list_id, None)
        db_filename = self.get_db_filename(list_id)
        if os.path.isfile(db_filename):
            os.unlink(db_filename)
