import base64
import logging
import os

from chronos.cbapi import CbApi
from chronos.config import config

from requests.exceptions import HTTPError

def lock_type():
    return 'carbon_black'

def on_lock_timeout(task):
    task.delay(60, True)

def process(task):
    # read request
    hostname = task.request['hostname']
    content = task.request['content']
    destination = task.request['destination']

    # create file from request
    task.create_file("content", base64.b64decode(content), clobber=False)

    # create cb interface
    url = config.get('carbon_black', 'url')
    token = config.get('carbon_black', 'token')
    cb = CbApi(url, ssl_verify=False, token=token)

    # get sensor id if we do not already have one
    sensors = cb.sensors({"hostname":hostname})
    if len(sensors) > 0:
        sensor_id = sensors[0]['id']
        logging.debug("resolved {} to sensor {}".format(hostname, sensor_id))
    else:
        return task.fail("could not find sensor for {}".format(hostname))

    # get session if we do not already have one
    session = None
    for sess in cb.live_response_session_list():
        if sess['sensor_id'] == sensor_id and (sess['status'] == 'active' or sess['status'] == 'pending'):
            logging.debug("found existing session")
            session = sess
            break
    if session is None:
        logging.debug("no existing sessions found. creating new session")
        session = cb.live_response_session_create(sensor_id)

    session_id = session['id']
    logging.debug("using session {}".format(session_id))

    # check session status
    status = session['status']
    logging.debug("session status is {}".format(status))
    if status == 'pending':
        return task.delay(10)
    elif status != 'active':
        return task.delay(0)

    # get command if we do not already have one
    command = None
    for cmd in cb.live_response_session_command_list(session_id):
        if cmd['name'] == 'put file' and cmd['object'] == destination:
            logging.debug("found existing command")
            command = cmd
            break
    if command is None:
        logging.debug("no existing commands found")

        # upload the file to carbon black
        logging.debug("uploading file to carbon black")
        path = os.path.join(task.directory, "content")
        file_id = cb.live_response_session_command_put_file(session_id, path)

        logging.debug("creating new command")
        options = [destination, {"file_id":file_id}]
        command = cb.live_response_session_command_post(session_id, 'put file', options)

    command_id = command['id']
    logging.debug("using command {}".format(command_id))

    # check command status
    status = command['status']
    logging.debug("command status is {}".format(status))
    if status == 'pending':
        return task.delay(10)
    elif status == 'error':
        return task.fail("{} ({}): {}".format(command['result_type'], command['result_code'], command['result_desc']))
    elif status != 'complete':
        return task.delay(0)
