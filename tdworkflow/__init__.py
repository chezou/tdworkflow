import logging
from importlib.metadata import PackageNotFoundError, version

from . import (
    attempt as attempt,
)
from . import (
    client as client,
)
from . import (
    exceptions as exceptions,
)
from . import (
    log as log,
)
from . import (
    project as project,
)
from . import (
    revision as revision,
)
from . import (
    schedule as schedule,
)
from . import (
    session as session,
)
from . import (
    workflow as workflow,
)

try:
    __version__ = version("tdworkflow")
except PackageNotFoundError:
    # package is not installed
    __version__ = "0.0.0+unknown"


logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)
