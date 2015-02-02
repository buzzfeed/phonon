__author__ = 'Andrew Kelleher'

import logging
import sys

SYSLOG_LEVEL = logging.WARNING

def get_logger(name, log_level=SYSLOG_LEVEL):
    """
    Sets up a logger to syslog at a given log level with the standard log format.

    :param str name: The name for the logger
    :param int log_level: Should be one of logging.INFO, logging.WARNING, etc.

    :returns: A logger implementing warning, error, info, etc.
    :rtype: logging.Logger
    """
    l = logging.getLogger(name)

    formatter = logging.Formatter(fmt='PHONON %(levelname)s - ( %(pathname)s ):%(funcName)s:L%(lineno)d %(message)s')
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    l.addHandler(handler)
    l.propagate = True
    l.setLevel(log_level)

    return l
