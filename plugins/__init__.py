import importlib
import logging
import os, os.path

import chronos

plugins = {}

logging.info("importing plugins")
files = os.listdir(os.path.join(chronos.CHRONOS_HOME, "plugins"))
for file in files:
    # skip directories and files wihtout .py extension
    path = os.path.join(chronos.CHRONOS_HOME, "plugins", file)
    if not os.path.isfile(path):
        continue

    name, ext = os.path.splitext(file)
    if ext != ".py":
        continue

    if name == "__init__":
        continue

    # import the plugin
    plugin_path = "plugins.{}".format(name)
    logging.debug("importing plugin {}".format(plugin_path))
    plugin = importlib.import_module(plugin_path)
    plugins[name] = plugin
