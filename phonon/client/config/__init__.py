import pytz
import math
import logging

from collections import defaultdict

from phonon.exceptions import ArgumentError
from phonon.client.config.node import Node
from phonon.client.config.shard import Shards, Shard
from phonon.logger import get_logger

LOCAL_TZ = pytz.utc
PHONON_NAMESPACE = "phonon"
SYSLOG_LEVEL = logging.warning
SHARDS = None
TTL = 30 * 60

logger = get_logger(__name__)


def default_quorum_size(shard_size=None):
    """
    Figures out the minimum quorum size for a given shard size assuming the nodes are split evenly between regions.

    :param int shard_size: The size of the shard.

    :rtype: int
    :returns: The minimum viable quorum size.
    """
    if not shard_size or shard_size < 2:
        raise ArgumentError("Shard size is required")
    quorum_size = int(math.ceil(float(shard_size) / 2.))
    if shard_size % quorum_size == 0:
        return quorum_size + 1
    return quorum_size


def default_shard_size(config):
    """
    Figures out the smallest viable shard size based on the config passed. The size is determined is twice the number of nodes in the region with the smallest amount of nodes.

    :param dict config: The configuration of the form {"region_name": ["hostname", ...]}

    :returns: The default size for the shards based on the configuration passed.
    :rtype: int
    """
    region_sizes = []
    for region, hostnames in config.items():
        if isinstance(hostnames, list):
            region_sizes.append(len(hostnames))
    region_sizes.sort()
    return 2 * min(region_sizes)


def config_to_nodelist(config):
    """
    Converts a configuration of hostnames to the same data structure with the hostnames replaced by phonon.node.Node objects.

    :param dict config: The configuration of the form {"region_name": ["hostname", ...]}

    :returns: A nodelist. a dictionary of lists of nodes by region
    """
    nodelist = defaultdict(list)
    for region, hostnames in config.items():
        if isinstance(hostnames, list):
            for hostname in hostnames:
                nodelist[region].append(Node(hostname=hostname, region=region))
    return nodelist


def configure(config, quorum_size=None, shard_size=None, shards=100, log_level=logging.WARNING):
    """
    Configures the global topology. The configuration you pass should be well balanced by region. You can specify a number of shards, but it should be much more than you think you'll ever need for the foreseeable future. There's no reason not to specify a large number. Really, be generous. You'll have to live with this decision.

    :param dict config: The configuration of the form {"region_name": ["hostname", ...]}
    :param int quorum_size: If you want to enforce a quorum size greater than the default beware it's possible two regions could be paired that don't have enough nodes to establish a quorum together. This results in a deadlock for a subset of requests.
    :param int shard_size:
    :param int shards:
    :param int log_level:
    """
    global SYSLOG_LEVEL
    global SHARDS
    SYSLOG_LEVEL = log_level
    shard_size = shard_size or default_shard_size(config)
    quorum_size = quorum_size or default_quorum_size(shard_size)
    SHARDS = Shards(nodelist=config_to_nodelist(config),
                    shards=shards,
                    quorum_size=quorum_size,
                    shard_size=shard_size)
