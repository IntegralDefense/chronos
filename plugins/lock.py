import logging

from chronos.cbapi import CbApi
from chronos.config import config

def lock_type():
    return 'default'

def on_lock_timeout(task):
    task.complete()

def process(task):
    return task.delay(1)
