import logging
import logging.config
import os, os.path

from chronos import CHRONOS_HOME
from configparser import ConfigParser

# required directories
tasks_dir = os.path.join(CHRONOS_HOME, 'tasks')
logs_dir = os.path.join(CHRONOS_HOME, 'logs')
if not os.path.isdir(tasks_dir):
    os.mkdir(tasks_dir)
if not os.path.isdir(logs_dir):
    os.mkdir(logs_dir)

# initialize loggin
try:
    config_path = os.path.join(CHRONOS_HOME, "config/logging.ini")
    logging.config.fileConfig(config_path)
except Exception as e:
    print("unable to load logging configuration: {}".format(e))
    raise e

# initialize config
config = None
try:
    temp = ConfigParser(allow_no_value=True)
    config_path = os.path.join(CHRONOS_HOME, 'config/chronos.ini')
    temp.read(config_path)
    config = temp
except Exception as e:
    logging.error("unable to load configuration: {}".format(e))
