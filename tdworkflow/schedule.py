import dataclasses

from typing import Dict, List

from .workflow import Workflow
from .project import Project
from .attempt import Attempt


@dataclasses.dataclass
class Schedule:
    id: int
    project: Project
    workflow: Workflow
    updatedAt: str = ""
    disableAt: str = ""
    nextScheduleTime: Dict = None
    nextRunTime: str = ""

    def __post_init__(self):
        self.id = int(self.id)
        self.project = Project(**self.project)
        self.workflow = Workflow(**self.workflow)


@dataclasses.dataclass
class ScheduleAttempt:
    id: int
    attempts: List[Attempt]
    project: Project = None
    workflow: Workflow = None

    def __post_init__(self):
        self.id = int(self.id)
        self.attempts = [Attempt(**att) for att in self.attempts]
        self.project = Project(**self.pro)
        self.workflow = Project(**self.workflow)