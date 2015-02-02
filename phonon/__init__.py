import pytz
import logging
import sys

LOCAL_TZ = pytz.utc
PHONON_NAMESPACE = "phonon"


SYSLOG_LEVEL = logging.WARNING

class DisRefError(Exception):
    pass

def get_logger(name, log_level=SYSLOG_LEVEL):
    l = logging.getLogger(name)

    formatter = logging.Formatter(fmt='PHONON %(levelname)s - ( %(pathname)s ):%(funcName)s:L%(lineno)d %(message)s')
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    l.addHandler(handler)
    l.propagate = True
    l.setLevel(log_level)

    return l
