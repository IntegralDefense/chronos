#!/usr/bin/env python
import os
import os.path
import sys

# XXX
chronos_home = '/opt/chronos'
sys.path.append(chronos_home)

# initialize flask
from chronos_web_service import chronos
application = chronos
