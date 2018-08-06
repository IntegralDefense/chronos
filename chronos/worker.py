import logging
import traceback
import time

from datetime import timedelta, datetime
from multiprocessing import Process, Event
from signal import signal, SIGTERM, SIGINT

from chronos.config import config
from chronos.database import Database
from plugins import plugins

def ignore_signal(signum, frame):
    pass

class Worker(Process):
    def __init__(self, lock):
        Process.__init__(self)
        self.lock = lock
        self.exit = Event()

    def run(self):
        # ignore term and int signals, use shutdown function to quit
        signal(SIGTERM, ignore_signal)
        signal(SIGINT, ignore_signal)

        # connect to the database
        with Database() as db:
            # keep processing until told to stop
            while not self.exit.is_set():
                # get the next task using a lock to prevent duplicate processing
                self.lock.acquire()
                task = db.get_next_task()
                self.lock.release()

                # continue if the are no available tasks
                if task is None:
                    time.sleep(1)
                    continue

                try:
                    # get the plugin for this task
                    plugin = plugins[task.plugin]

                    # determine timeout time
                    timeout = config.getint("lock_type_default", "timeout")
                    if config.has_option("lock_type_{}".format(task.lock_type), "timeout"):
                        timeout = config.getint("lock_type_{}".format(task.lock_type), "timeout")
                    timeout_time = task.locked_at + timedelta(seconds=timeout)

                    # check if task has timed out
                    if datetime.now() > timeout_time:
                        logging.info("task {} lock timed out".format(task.id))
                        plugin.on_lock_timeout(task)
                        continue

                    # run the task
                    status = plugin.process(task)

                    # if the plugin did not return a status then mark task as complete
                    if status is None:
                        task.complete()

                # report plugin failure
                except Exception as e:
                    task.fail(str(e))
                    traceback.print_exc()


    # safely stops the worker
    def shutdown(self):
        self.exit.set()
