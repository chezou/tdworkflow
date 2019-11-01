import logging

import pkg_resources

from . import (
    attempt,
    client,
    exceptions,
    log,
    project,
    revision,
    schedule,
    session,
    workflow,
)

try:
    __version__ = pkg_resources.get_distribution(__name__).version
except pkg_resources.DistributionNotFound:
    # package is not installed
    pass


logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)
