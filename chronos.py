#!/usr/bin/env python3
import logging
import os
import sys
import argparse
import os
import time

from multiprocessing import Lock, Event
from signal import signal, SIGTERM, SIGINT, SIGCHLD

import chronos
from chronos.config import config
from chronos.database import Database
from chronos.worker import Worker

sys.dont_write_bytecode = True

workers = []
exit = Event()
lock = Lock()

def worker_died(signum, frame):
    if not exit.is_set():
        logging.warning("worker quit unexpectidly, spawning new worker")
        spawn_worker()

def spawn_worker():
    worker = Worker(lock)
    worker.start()
    workers.append(worker)
    logging.debug("worker {} started".format(worker.pid))

parser = argparse.ArgumentParser(description="Chronos")
parser.add_argument('--chronos-home', required=False, dest='chronos_home', default=None,
    help="Sets the location of chronos. Defaults to /opt/chronos")
parser.add_argument('-k', '--kill-daemon', required=False, dest='kill_daemon', default=False, action='store_true',
    help="Kill the currently processing process.")
parser.add_argument('-b', '--background', required=False, dest='background', default=False, action='store_true',
    help="Run as a background process.")
args = parser.parse_args()

if args.chronos_home:
    chronos.CHRONOS_HOME = args.chronos_home

# this is built to run out of this directory
try:
    os.chdir(chronos.CHRONOS_HOME)
except Exception as e:
    sys.stderr.write("unable to cd to {}: {}\n".format(chronos.CHRONOS_HOME, e))
    sys.exit(1)

pid_file = os.path.join(chronos.CHRONOS_HOME, 'chronos.pid')
if args.kill_daemon:
    if os.path.exists(pid_file):
        # is it still running?
        import psutil
        with open(pid_file, 'r') as fp:
            pid = int(fp.read().strip())

        if not psutil.pid_exists(pid):
            print("removing stale pid file")
            try:
                os.remove(pid_file)
                sys.exit(0)
            except Exception as e:
                sys.stderr.write("unable to delete stale pid file {}: {}\n".format(pid_file, e))
                sys.exit(1)

        # kill it
        p = psutil.Process(pid)
        try:
            print("terminating process {}".format(pid))
            p.terminate()
            p.wait(5)

            try:
                os.remove(pid_file)
            except Exception as e:
                sys.stderr.write("unable to delete pid file {}: {}\n".format(pid_file, e))
            
        except Exception as e:
            print("killing process {}".format(pid))
            try:
                p.kill()
                p.wait(1)

                try:
                    os.remove(pid_file)
                except Exception as e:
                    sys.stderr.write("unable to delete pid file {}: {}\n".format(pid_file, e))

            except Exception as e:
                sys.stderr.write("unable to kill process {}\n".format(pid))
                sys.exit(1)

        sys.exit(0)
    else:
        print("no process running")
        sys.exit(0)

if os.path.exists(pid_file):
    print("existing process running or stale pid file (use -k to clear or kill)")
    sys.exit(1)

# are we running as a deamon/
if args.background:
    pid = None

    # http://code.activestate.com/recipes/278731-creating-a-daemon-the-python-way/
    try:
        pid = os.fork()
    except OSError as e:
        logging.fatal("{} ({})".format(e.strerror, e.errno))
        sys.exit(1)

    if pid == 0:
        os.setsid()

        try:
            pid = os.fork()
        except OSError as e:
            logging.fatal("{} ({})".format(e.strerror, e.errno))
            sys.exit(1)

        if pid > 0:
            # write the pid to a file
            with open(pid_file, 'w') as fp:
                fp.write(str(pid))

            print("background pid = {}".format(pid))

            os._exit(0)
    else:
        os._exit(0)

    import resource
    maxfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
    if (maxfd == resource.RLIM_INFINITY):
        maxfd = MAXFD

        for fd in range(0, maxfd):
            try:
                os.close(fd)
            except OSError:   # ERROR, fd wasn't open to begin with (ignored)
                pass

    if (hasattr(os, "devnull")):
        REDIRECT_TO = os.devnull
    else:
        REDIRECT_TO = "/dev/null"

    os.open(REDIRECT_TO, os.O_RDWR)
    os.dup2(0, 1)
    os.dup2(0, 2)

shutdown = False

def handler(signum, frame):
    global shutdown
    shutdown = True

signal(SIGTERM, handler)
signal(SIGINT, handler)

try:
    with Database() as db:
        # requeue any tasks that were left in the running state
        db.queue_running_tasks()

        # spawn configured number of threads
        for index in range(config.getint("chronos", "num_workers")):
            logging.info("starting worker")
            spawn_worker()

        # manage locks
        while not shutdown:
            try:
                locks = db.select_locks()
                if not locks:
                    time.sleep(1)
                    continue

                for l in locks:
                    # get lock info
                    lock_type = l[0]
                    lock_count = l[1]

                    # determine max number locks allowed for this lock_type
                    max_locks = config.getint("lock_type_default", "max_locks")
                    if config.has_option("lock_type_{}".format(lock_type), "max_locks"):
                        max_locks = config.getint("lock_type_{}".format(lock_type), "max_locks")

                    # lock as many tasks of this lock type as needed to use all available locks
                    if lock_count < max_locks:
                        db.lock_oldest(lock_type, max_locks - lock_count)
            except Exception as e:
                logging.error("uncaught exception: {}".format(e))
                time.sleep(1)

except KeyboardInterrupt:
    pass

logging.info("shutting down workers")
for worker in workers:
    logging.debug("shutting down worker {}".format(worker.pid))
    worker.shutdown()
    worker.join()

logging.info("shutdown successful")
