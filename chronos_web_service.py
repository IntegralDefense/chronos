#!/usr/bin/env python3
import json
import logging
import os
import sys

from datetime import datetime
from hashlib import md5

from chronos.config import config
from chronos.database import Database
from plugins import plugins

from flask import Flask, abort, request

sys.dont_write_bytecode = True

# The web service
chronos = Flask(__name__)

# hello world page that displays some simple stats
@chronos.route("/chronos")
def display_stats():
    stats = ""
    with Database() as db:
        for stat in db.select_statistics():
            stats += "{}: {}\n".format(stat[0], stat[1])
    return stats

# page that returns the status of the task with given id
@chronos.route("/chronos/task/status/<id>")
def task_status(id):
    status = None
    with Database() as db:
        status = db.select_status(id)
    if status is None:
        abort(404)
    return status

# queue new task and return the id
# if the task already exists then it is not queued, just the id is returned
@chronos.route("/chronos/task/create/<plugin>", methods = ["POST"])
def task_create(plugin):
    logging.debug("request for new {} task".format(plugin))
    # make sure plugin exists
    if plugin not in plugins:
        logging.error("plugin not found: {}".format(plugin))
        abort(404)

    # get posted json
    logging.debug("loading request json")
    request_json = request.get_json(force=True)

    # calculate task id
    logging.debug("calculating task id")
    md5_hasher = md5()
    md5_hasher.update(plugin.encode('utf-8'))
    md5_hasher.update(json.dumps(request_json).encode('utf-8'))
    id = md5_hasher.hexdigest().upper()
    logging.debug("task id = {}".format(id))

    # create task dir if it does not exist already
    task_dir = os.path.join("tasks", id[0:2])
    if not os.path.isdir(task_dir):
        os.mkdir(task_dir)
    task_dir = os.path.join(task_dir, id)
    if not os.path.isdir(task_dir):
        os.mkdir(task_dir)

    # store request json in task dir
    request_file = os.path.join(task_dir, 'request.json')
    if not os.path.isfile(request_file):
        with open(request_file, "w") as fp:
            json.dump(request_json, fp)

    lock_type = plugins[plugin].lock_type()

    # create new task
    with Database() as db:
        db.insert_task(id, plugin, lock_type)

    # return task id
    return id

@chronos.route("/chronos/task/result/<id>", methods = ["GET"])
def task_result(id):
    result_path = os.path.join("tasks", id[0:2], id, "result")
    if not os.path.isfile(result_path):
        abort(404)
    result = ""
    with open(result_path, "rb") as fh:
        result = fh.read()
    return result

# resets the time out of the lock
@chronos.route("/chronos/lock/keep_alive/<id>", methods = ["GET"])
def lock_keep_alive(id):
    with Database() as db:
        status = db.lock_keep_alive(id)
    return ""

# returns 0 if task with id is not locked and 1 if it is
@chronos.route("/chronos/lock/status/<id>", methods = ["GET"])
def lock_status(id):
    status = None
    with Database() as db:
        status = db.select_lock_status(id)
    if status is None:
        abort(404)
    return str(status)

# creates a new lock task and returns the id
@chronos.route("/chronos/lock/acquire/<lock_type>", methods = ["GET"])
def lock_acquire(lock_type):
    logging.debug("request for new {} lock".format(lock_type))

    # calculate unique id
    # TODO: loop until we get unique id?
    logging.debug("calculating task id")
    md5_hasher = md5()
    idstr = "{}{}".format(lock_type, datetime.now())
    md5_hasher.update(idstr.encode('utf-8'))
    id = md5_hasher.hexdigest().upper()
    logging.debug("task id = {}".format(id))

    # create new task
    with Database() as db:
        db.insert_task(id, "lock", lock_type)

    # return task id
    return id

@chronos.route("/chronos/lock/release/<id>", methods = ['GET'])
def lock_release(id):
    # complete the task
    with Database() as db:
        db.complete_task(id)
    return ""

# handles internal server errors
@chronos.errorhandler(500)
def internal_error(error):
    logging.error("Internal Server Error - {}".format(error))
    return "(500) Internal Server Error\n"

# start the web service if running from cmdline
if __name__ == "__main__":
    port = config.getint("chronos", "port")
    chronos.run(host='0.0.0.0', port=port)
