import logging

from chronos.config import config
from chronos.task import Task

import pymysql

# load mysql info
host = config.get('mysql', 'host')
db = config.get('mysql', 'db')
user = config.get('mysql', 'user')
passwd = config.get('mysql', 'passwd')
unix_socket = config.get('mysql', 'unix_socket', fallback=None)

# task.status eum
STATUS_QUEUED = 'queued'
STATUS_RUNNING = 'running'
STATUS_COMPLETE = 'complete'
STATUS_ERROR = 'error'

class Database:
    _conn = None

    @property
    def connection(self):
        if not self.is_connected():
            self.connect()
        return self._conn

    def __init__(self):
        self.connect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.disconnect()

    def is_connected(self):
        connected = False
        try:
            c = self._conn.cursor()
            c.execute("DO 1")
            connected = True
        except:
            pass
        return connected

    def connect(self):
        try:
            self.disconnect()
            #self._conn = MySQLdb.connect(host=host, db=db, user=user, passwd=passwd, charset='utf8')
            self._conn = pymysql.connect(host=host, db=db, user=user, passwd=passwd, unix_socket=unix_socket, charset='utf8')
            
        except Exception as e:
            logging.error("failed to connect to database: {}".format(str(e)))

    def disconnect(self):
        if self._conn is not None:
            self._conn.close()

    def select_locks(self):
        locks = []
        try:
            c = self.connection.cursor()
            c.execute("""
                SELECT lock_type, sum(locked)
                FROM tasks
                WHERE status = %s OR status = %s
                GROUP BY lock_type
            """, (STATUS_QUEUED, STATUS_RUNNING))
            locks = c.fetchall()
            self.connection.commit()
        except Exception as e:
            logging.error("failed to select locks: {}".format(str(e)))
        return locks

    def lock_oldest(self, lock_type, count):
        try:
            c = self.connection.cursor()
            c.execute("""
                UPDATE tasks
                SET locked = 1, locked_at = now()
                WHERE locked = 0 AND status = %s AND lock_type = %s AND modified <= now()
                ORDER BY created
                LIMIT %s
                """, (STATUS_QUEUED, lock_type, int(count)))
            self.connection.commit()
        except Exception as e:
            logging.error("failed to get lock {} for {} tasks: {}".format(lock_type, count, str(e)))

    def get_next_task(self):
        row = None
        try:
            c = self.connection.cursor()
            c.execute("""
                SELECT HEX(id), plugin, lock_type, locked_at
                FROM tasks
                WHERE status = %s AND modified <= now() AND locked = 1
                ORDER BY created
                LIMIT 1
            """, (STATUS_QUEUED))
            row = c.fetchone()
            self.connection.commit()
        except Exception as e:
            logging.error("failed to get next task: {}".format(str(e)))
        if row is None:
            return None
        task = Task(self, row[0], row[1], row[2], row[3])
        self.start_task(task.id)
        return task

    def insert_task(self, id, plugin, lock_type):
        try:
            c = self.connection.cursor()
            c.execute("""
                INSERT IGNORE INTO tasks (id, plugin, lock_type, status, created)
                VALUES (UNHEX(%s), %s, %s, %s, now())
                """, (id, plugin, lock_type, STATUS_QUEUED))
            self.connection.commit()
        except Exception as e:
            logging.error("failed to insert task {}: {}".format(id, str(e)))

    def select_status(self, id):
        row = None
        try:
            c = self.connection.cursor()
            c.execute("""
                SELECT status
                FROM tasks
                WHERE id = UNHEX(%s)
                """, (id))
            row = c.fetchone()
            self.connection.commit()
        except Exception as e:
            logging.error("failed to select status of task {}: {}".format(id, str(e)))
        if row is None:
            return None
        return row[0]

    def select_lock_status(self, id):
        row = None
        try:
            c = self.connection.cursor()
            c.execute("""
                SELECT locked
                FROM tasks
                WHERE id = UNHEX(%s)
                """, (id))
            row = c.fetchone()
            self.connection.commit()
        except Exception as e:
            logging.error("failed to select lock status of task {}: {}".format(id, str(e)))
        if row is None:
            return None
        return row[0]

    def lock_keep_alive(self, id):
        try:
            c = self.connection.cursor()
            c.execute("""
                UPDATE tasks
                SET locked_at = now()
                WHERE id = UNHEX(%s)
                """, (id))
            self.connection.commit()
        except Exception as e:
            logging.error("failed to keep lock alive {}: {}".format(id, str(e)))

    def update_status(self, id, status, unlock=False):
        lock_status =  0 if unlock else 1
        try:
            c = self.connection.cursor()
            c.execute("""
                UPDATE tasks
                SET status = %s, locked = %s
                WHERE id = UNHEX(%s)
                """, (status, lock_status, id))
            self.connection.commit()
        except Exception as e:
            logging.error("failed to update status of task {}: {}".format(id, str(e)))

    def start_task(self, id):
        logging.info("running task {}".format(id))
        self.update_status(id, STATUS_RUNNING)
        return STATUS_RUNNING

    def delay_task(self, id, seconds, unlock=False):
        logging.info("delaying task {} for {} seconds".format(id, seconds))
        lock_status =  0 if unlock else 1
        try:
            c= self.connection.cursor()
            c.execute("""
                UPDATE tasks
                SET status = %s, modified = now() + INTERVAL %s SECOND, locked = %s
                WHERE id = UNHEX(%s)
                """, (STATUS_QUEUED, seconds, lock_status, id))
            self.connection.commit()
        except Exception as e:
            logging.error("failed to delay task {}: {}".format(id, str(e)))
        return STATUS_QUEUED

    def fail_task(self, id, error):
        logging.error("task {} failed: {}".format(id, error))
        self.update_status(id, STATUS_ERROR, True)
        return STATUS_ERROR

    def complete_task(self, id):
        logging.info("task {} complete".format(id))
        self.update_status(id, STATUS_COMPLETE, True)
        return STATUS_COMPLETE

    def select_statistics(self):
        stats = []
        try:
            c = self.connection.cursor()
            c.execute("""
                SELECT status, count(*)
                FROM tasks
                GROUP BY status
            """)
            stats = c.fetchall()
            self.connection.commit()
        except Exception as e:
            logging.error("failed to select statistics: {}".format(str(e)))
        return stats

    def queue_running_tasks(self):
        try:
            c = self.connection.cursor()
            c.execute("""
                UPDATE tasks
                SET status = %s
                WHERE status = %s
                """, (STATUS_QUEUED, STATUS_RUNNING))
            self.connection.commit()
        except Exception as e:
            logging.error("failed to queue running tasks: {}".format(str(e)))
