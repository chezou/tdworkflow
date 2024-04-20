import logging
from importlib.metadata import PackageNotFoundError, version

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
    __version__ = version("tdworkflow")
except PackageNotFoundError:
    # package is not installed
    pass


logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)
