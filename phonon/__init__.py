import pytz
import math
import logging
import sys
from collections import defaultdict

from phonon.exceptions import ConfigError, ArgumentError

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

def default_quorum_size(shard_size=None):
    if not shard_size:
        raise ArgumentError("Shard size is required")
    quorum_size = int(math.ceil(float(shard_size) / 2.))
    if shard_size % quorum_size == 0:
        return quorum_size + 1
    return quorum_size

def default_shard_size(config):
    region_sizes = []
    for region, hostnames in config.items():
        if isinstance(hostnames, list):
            region_sizes.append(len(hostnames))
    region_sizes.sort()
    return 2 * min(region_sizes)

def config_to_nodelist(config):
    nodelist = defaultdict(list)
    for region, hostnames in config.items():
        if isinstance(hostnames, list):
            for hostname in hostnames:
                nodelist[region].append(Node(hostname=hostname, region=region))
    return nodelist

def configure(config, quorum_size=None, shard_size=None, shards=100, log_level=logging.WARNING):
    global SYSLOG_LEVEL
    global TOPOLOGY
    SYSLOG_LEVEL = log_level
    shard_size = shard_size or default_shard_size(config)
    quorum_size = quorum_size or default_quorum_size(shard_size)
    TOPOLOGY = Shards(nodelist=config_to_nodelist(config),
                      shards=shards,
                      quorum_size=quorum_size,
                      shard_size=shard_size)
