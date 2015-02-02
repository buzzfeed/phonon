import pytz
import math
import logging
import sys
from collections import defaultdict

from phonon.exceptions import ConfigError

from phonon.config.node import Node
from phonon.config.shard import Shards, Shard

LOCAL_TZ = pytz.utc
PHONON_NAMESPACE = "phonon"
SYSLOG_LEVEL = logging.WARNING
TOPOLOGY = None

def get_logger(name, log_level=SYSLOG_LEVEL):
    l = logging.getLogger(name)

    formatter = logging.Formatter(fmt='PHONON %(levelname)s - ( %(pathname)s ):%(funcName)s:L%(lineno)d %(message)s')
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    l.addHandler(handler)
    l.propagate = True
    l.setLevel(log_level)

    return l

def qsize(num_nodes):
    default_quorum_size = int(math.ceil(num_nodes/2.))
    if num_nodes / 2. % default_quorum_size == 0.:
        default_quorum_size = int(default_quorum_size + 1.)
    return default_quorum_size

def configure(config, log_level=logging.WARNING, shards=100):
    global SYSLOG_LEVEL
    global TOPOLOGY

    SYSLOG_LEVEL = log_level

    nodelist = defaultdict(list)
    num_nodes = 0
    for region, hostnames in config.items():
        for hostname in hostnames:
            num_nodes += 1
            nodelist[region].append(Node(hostname=hostname, region=region))

    if num_nodes < 4 or len(nodelist.keys()) < 2:
        raise ConfigError("You have to have at least 4 nodes, 2 each in different data centers")

    TOPOLOGY = Shards(nodelist=nodelist, num=shards, quorum_size=config.get('quorum_size', qsize(num_nodes)))

