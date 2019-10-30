import dataclasses

from typing import Dict

from .workflow import Workflow
from .project import Project


@dataclasses.dataclass
class Attempt:
    id: int
    sessionId: int = -1
    sessionUuid: str = ""
    sessionTime: str = ""
    workflow: Workflow = None
    project: Project = None
    index: int = -1
    retryAttemptName: str = ""
    done: bool = False
    success: bool = False
    cancelRequested: bool = False
    params: Dict = None
    createdAt: str = ""
    finishedAt: str = ""

    def __post_init__(self):
        self.id = int(self.id)
        self.project = Project(**self.project) if self.project else None
        self.workflow = Workflow(**self.workflow) if self.workflow else None

    @property
    def session_id(self):
        return self.sessionId

    @property
    def session_uuid(self):
        return self.sessionUuid

    @property
    def sesesion_time(self):
        return self.sessionTime

    @property
    def retry_attempt_name(self):
        return self.retryAttemptName

    @property
    def cancel_requested(self):
        return self.cancelRequested

    @property
    def finished_at(self):
        return self.finishedAt

    def finished(self):
        return bool(self.finished_at)
