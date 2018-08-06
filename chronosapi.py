import base64
import json
import time

import requests

try:
    from requests.packages.urllib3.exceptions import InsecureRequestWarning
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
except:
    pass

NO_LOCK = '0'
STATUS_LOCKED = '1'
STATUS_UNLOCKED = '0'

class Chronos(object):
    def __init__(self, server):
        self.server = server.rstrip("/")
        if not server.startswith("http"):
            self.server = "http://{}".format(self.server)
        self.session = requests.Session()

    def lock_acquire(self, lock_type, block=True, callback=None):
        r = self.session.get("%s/chronos/lock/acquire/%s" % (self.server, lock_type))
        r.raise_for_status()
        lock_id = r.text
        if block:
            while self.lock_status(lock_id) == "0":
                if callback and callback():
                    self.lock_release(lock_id)
                    return None
                time.sleep(1)
        return lock_id

    def lock_status(self, lock_id):
        r = self.session.get("%s/chronos/lock/status/%s" % (self.server, lock_id))
        r.raise_for_status()
        return r.text

    def lock_keep_alive(self, lock_id):
        r = self.session.get("%s/chronos/lock/keep_alive/%s" % (self.server, lock_id))
        r.raise_for_status()

    def lock_release(self, lock_id):
        r = self.session.get("%s/chronos/lock/release/%s" % (self.server, lock_id))
        r.raise_for_status()

    def task_create(self, task_type, data):
        headers = {"Content-Type":"application/json"}
        jdata = json.dumps(data)
        r = self.session.post("%s/chronos/task/create/%s" % (self.server, task_type), headers=headers, data=jdata)
        r.raise_for_status()
        return r.text

    def task_status(self, task_id):
        r = self.session.get("%s/chronos/task/status/%s" % (self.server, task_id))
        r.raise_for_status()
        return r.text

    def task_result(self, task_id):
        r = self.session.get("%s/chronos/task/result/%s" % (self.server, task_id))
        r.raise_for_status()
        return r.content

    def collect_file(self, hostname, location):
        data = {"hostname":hostname,"location":location}
        return self.task_create("collect_file", data)

    def execute_command(self, hostname, cmdline):
        data = {"hostname":hostname,"cmdline":cmdline}
        return self.task_create("execute_command", data)

    def put_file(self, hostname, file_path, destination):
        content = ""
        with open(file_path, "rb") as fh:
            content = base64.b64encode(fh.read())
        data = {"hostname":hostname, "content":content, "destination":destination}
        return self.task_create("put_file", data)

    def remediate_emails(self, emails):
        data = {"emails":emails}
        return self.task_create("remediate_email", data)

    def create_event_wiki(self, name, alerts):
        data = {"name":name,"alerts":alerts}
        return self.task_create("event2wiki", data)
