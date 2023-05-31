import dataclasses
from datetime import datetime
from typing import Optional

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
    sessionTime: Optional[datetime] = None
    lastAttempt: Optional[Attempt] = None

    def __post_init__(self) -> None:
        self.id = int(self.id)
        self.project = Project(**self.project)  # type: ignore
        self.workflow = Workflow(**self.workflow)  # type: ignore
        self.sessionTime = parse_iso8601(self.sessionTime)  # type: ignore
        self.lastAttempt = Attempt(**self.lastAttempt)  # type: ignore

    @property
    def session_uuid(self) -> str:
        return self.sessionUuid

    @property
    def session_time(self) -> Optional[datetime]:
        return self.session_time

    @property
    def last_attempt(self) -> Optional[Attempt]:
        return self.lastAttempt
