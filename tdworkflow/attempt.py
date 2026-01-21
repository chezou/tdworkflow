import dataclasses
from datetime import datetime
from typing import Any

from .project import Project
from .resource import Resource
from .util import parse_iso8601
from .workflow import Workflow


@dataclasses.dataclass
class Attempt(Resource):
    id: int
    sessionId: int = -1
    sessionUuid: str = ""
    sessionTime: datetime | None = None
    workflow: Workflow | None = None
    project: Project | None = None
    index: int = -1
    retryAttemptName: str = ""
    done: bool = False
    success: bool = False
    cancelRequested: bool = False
    params: dict[str, Any] | None = None
    createdAt: datetime | None = None
    finishedAt: datetime | None = None
    status: str = ""

    def __post_init__(self) -> None:
        self.id = int(self.id)
        if self.sessionTime and isinstance(self.sessionTime, str):
            self.sessionTime = parse_iso8601(self.sessionTime)
        if self.project and isinstance(self.project, dict):
            self.project = Project(**self.project)
        if self.workflow and isinstance(self.workflow, dict):
            self.workflow = Workflow(**self.workflow)
        self.done = bool(self.done)
        self.success = bool(self.success)
        self.cancelRequested = bool(self.cancelRequested)
        if self.createdAt and isinstance(self.createdAt, str):
            self.createdAt = parse_iso8601(self.createdAt)
        if self.finishedAt and isinstance(self.finishedAt, str):
            self.finishedAt = parse_iso8601(self.finishedAt)
        self.status = self.status

    @property
    def session_id(self) -> int:
        return self.sessionId

    @property
    def session_uuid(self) -> str:
        return self.sessionUuid

    @property
    def session_time(self) -> datetime | None:
        return self.sessionTime

    @property
    def retry_attempt_name(self) -> str:
        return self.retryAttemptName

    @property
    def cancel_requested(self) -> bool:
        return self.cancelRequested

    @property
    def finished_at(self) -> datetime | None:
        return self.finishedAt

    def finished(self) -> bool:
        return bool(self.finished_at)

    def update(self, **args: Any) -> None:
        other_attempt = Attempt(**args)
        self.id = other_attempt.id
        self.sessionId = other_attempt.sessionId
        self.sessionUuid = other_attempt.sessionUuid
        self.workflow = other_attempt.workflow
        self.project = other_attempt.project
        self.index = other_attempt.index
        self.retryAttemptName = other_attempt.retryAttemptName
        self.done = other_attempt.done
        self.success = other_attempt.success
        self.cancelRequested = other_attempt.cancelRequested
        self.params = other_attempt.params
        self.createdAt = other_attempt.createdAt
        self.finishedAt = other_attempt.finishedAt
        self.status = other_attempt.status
