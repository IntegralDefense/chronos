import json
import os

class Task(object):
    @property
    def request(self):
        if self._request is None:
            request_path = os.path.join(self.directory, "request.json")
            with open(request_path, "r") as fh:
                self._request = json.load(fh)
        return self._request

    def __init__(self, database, id, plugin, lock_type, locked_at):
        self.database = database
        self.id = id
        self.plugin = plugin
        self.lock_type = lock_type
        self.locked_at = locked_at
        self.directory = os.path.join("tasks", self.id[0:2], self.id)
        self._request = None

    def get_var(self, name):
        path = os.path.join(self.directory, name)
        value = {name:None}
        if os.path.isfile(path):
            with open(path, "r") as fh:
                value = json.load(fh)
        return value[name]

    def set_var(self, name, value):
        path = os.path.join(self.directory, name)
        with open(path, "w") as fh:
            json.dump({name:value}, fh)

    def create_file(self, rel_path, content, clobber=True):
        path = os.path.join(self.directory, rel_path)
        if not os.path.isfile(path) or clobber:
            with open(path, "wb") as fh:
                fh.write(content)

    def delay(self, seconds, unlock=False):
        return self.database.delay_task(self.id, seconds, unlock)

    def fail(self, error):
        return self.database.fail_task(self.id, error)

    def complete(self):
        return self.database.complete_task(self.id)
