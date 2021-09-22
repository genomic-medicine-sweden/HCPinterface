import logging
import os
from datetime import datetime


__version__ = '2.1.3'
logformat =  logging.Formatter("[%(asctime)s] - %(levelname)s: %(message)s")

# File work directory
WD = os.path.dirname(os.path.realpath(__file__))
TIMESTAMP = datetime.now().strftime("%y%m%d-%H%M%S")

# Initialize log
log = logging.getLogger("main_log")
log.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(logformat)
log.addHandler(ch)
