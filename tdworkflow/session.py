import dataclasses
from datetime import datetime

from .attempt import Attempt
from .project import Project
from .resource import Resource
from .util import parse_iso8601
from .workflow import Workflow


@dataclasses.dataclass
class Session(Resource):
    id: int
    project: Project
    workflow: Workflow
    sessionUuid: str
    sessionTime: datetime = None
    lastAttempt: Attempt = None

    def __post_init__(self):
        self.id = int(self.id)
        self.project = Project(**self.project)
        self.workflow = Workflow(**self.workflow)
        self.sessionTime = parse_iso8601(self.sessionTime)
        self.lastAttempt = Attempt(**self.lastAttempt)

    @property
    def session_uuid(self):
        return self.sessionUuid

    @property
    def session_time(self):
        return self.session_time

    @property
    def last_attempt(self):
        return self.lastAttempt
