import dataclasses
from typing import Dict, List

from .attempt import Attempt
from .project import Project
from .workflow import Workflow


@dataclasses.dataclass
class Schedule:
    id: int
    project: Project
    workflow: Workflow
    createdAt: str = ""
    updatedAt: str = ""
    disabledAt: str = ""
    nextScheduleTime: Dict = None
    nextRunTime: str = ""

    def __post_init__(self):
        self.id = int(self.id)
        self.project = Project(**self.project)
        self.workflow = Workflow(**self.workflow)

    @property
    def created_at(self):
        return self.createdAt

    @property
    def updated_at(self):
        return self.updatedAt

    @property
    def disabled_at(self):
        return self.disabledAt


@dataclasses.dataclass
class ScheduleAttempt:
    id: int
    attempts: List[Attempt]
    project: Project = None
    workflow: Workflow = None

    def __post_init__(self):
        self.id = int(self.id)
        self.attempts = [Attempt(**att) for att in self.attempts]
        self.project = Project(**self.project)
        self.workflow = Project(**self.workflow)
