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
    sessionTime: datetime | None = None
    lastAttempt: Attempt | None = None

    def __post_init__(self) -> None:
        self.id = int(self.id)
        if self.project and isinstance(self.project, dict):
            self.project = Project(**self.project)
        if self.workflow and isinstance(self.workflow, dict):
            self.workflow = Workflow(**self.workflow)
        if self.sessionTime and isinstance(self.sessionTime, str):
            self.sessionTime = parse_iso8601(self.sessionTime)
        if self.lastAttempt and isinstance(self.lastAttempt, dict):
            self.lastAttempt = Attempt(**self.lastAttempt)

    @property
    def session_uuid(self) -> str:
        return self.sessionUuid

    @property
    def session_time(self) -> datetime | None:
        return self.session_time

    @property
    def last_attempt(self) -> Attempt | None:
        return self.lastAttempt
