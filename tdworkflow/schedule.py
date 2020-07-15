import dataclasses
from datetime import datetime
from typing import Dict, List

from .attempt import Attempt
from .project import Project
from .resource import Resource
from .util import parse_iso8601
from .workflow import Workflow


@dataclasses.dataclass
class Schedule(Resource):
    id: int
    project: Project
    workflow: Workflow
    createdAt: datetime = None
    updatedAt: datetime = None
    disabledAt: datetime = None
    nextScheduleTime: Dict = None
    nextRunTime: datetime = None

    def __post_init__(self):
        self.id = int(self.id)
        self.project = Project(**self.project)
        self.workflow = Workflow(**self.workflow)
        self.createdAt = parse_iso8601(self.createdAt)
        self.updatedAt = parse_iso8601(self.updatedAt)
        self.disabledAt = parse_iso8601(self.disabledAt)
        self.nextRunTime = parse_iso8601(self.nextRunTime)

    @property
    def created_at(self):
        return self.createdAt

    @property
    def updated_at(self):
        return self.updatedAt

    @property
    def disabled_at(self):
        return self.disabledAt

    @property
    def next_run_time(self):
        return self.nextRunTime

    @property
    def next_schedule_time(self):
        return self.nextScheduleTime


@dataclasses.dataclass
class ScheduleAttempt(Resource):
    id: int
    attempts: List[Attempt]
    project: Project = None
    workflow: Workflow = None

    def __post_init__(self):
        self.id = int(self.id)
        self.attempts = [Attempt(**att) for att in self.attempts]
        self.project = Project(**self.project)
        self.workflow = Project(**self.workflow)
