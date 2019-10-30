import dataclasses

from .attempt import Attempt
from .project import Project
from .workflow import Workflow


@dataclasses.dataclass
class Session:
    id: int
    project: Project
    workflow: Workflow
    sessionUuid: str
    sessionTime: str
    lastAttempt: Attempt

    def __post_init__(self):
        self.id = int(self.id)
        self.project = Project(**self.project)
        self.workflow = Workflow(**self.workflow)
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
