from chronos.cbapi import CbApi
from chronos.config import config
from chronos.libevent2wiki import Event
import logging
import os
from requests.exceptions import HTTPError

def lock_type():
    return 'event2wiki'

def on_lock_timeout(task):
    task.complete()

def process(task):
    # read request
    name = task.request['name']
    alerts = task.request['alerts']
    Event.Event(event_name=name, alert_paths=alerts)
