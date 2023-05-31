import dataclasses
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from .attempt import Attempt
from .project import Project
from .resource import Resource
from .util import parse_iso8601
from .workflow import Workflow

NextSchedule = Dict[str, Union[int, str, Dict[str, Any]]]


@dataclasses.dataclass
class Schedule(Resource):
    id: int
    project: Project
    workflow: Workflow
    createdAt: Optional[datetime] = None
    updatedAt: Optional[datetime] = None
    disabledAt: Optional[datetime] = None
    nextScheduleTime: Optional[NextSchedule] = None
    nextRunTime: Optional[datetime] = None

    def __post_init__(self) -> None:
        self.id = int(self.id)
        if self.project and isinstance(self.project, dict):
            self.project = Project(**self.project)
        if self.workflow and isinstance(self.workflow, dict):
            self.workflow = Workflow(**self.workflow)
        if self.createdAt and isinstance(self.createdAt, str):
            self.createdAt = parse_iso8601(self.createdAt)
        if self.updatedAt and isinstance(self.updatedAt, str):
            self.updatedAt = parse_iso8601(self.updatedAt)
        if self.disabledAt and isinstance(self.disabledAt, str):
            self.disabledAt = parse_iso8601(self.disabledAt)
        if self.nextRunTime and isinstance(self.nextRunTime, str):
            self.nextRunTime = parse_iso8601(self.nextRunTime)

    @property
    def created_at(self) -> Optional[datetime]:
        return self.createdAt

    @property
    def updated_at(self) -> Optional[datetime]:
        return self.updatedAt

    @property
    def disabled_at(self) -> Optional[datetime]:
        return self.disabledAt

    @property
    def next_run_time(self) -> Optional[datetime]:
        return self.nextRunTime

    @property
    def next_schedule_time(self) -> Optional[NextSchedule]:
        return self.nextScheduleTime


@dataclasses.dataclass
class ScheduleAttempt(Resource):
    id: int
    attempts: List[Attempt]
    project: Optional[Project] = None
    workflow: Optional[Workflow] = None

    def __post_init__(self) -> None:
        self.id = int(self.id)
        self.attempts = [
            Attempt(**att) if isinstance(att, dict) else att for att in self.attempts
        ]
        if self.project and isinstance(self.project, dict):
            self.project = Project(**self.project)
        if self.workflow and isinstance(self.workflow, dict):
            self.workflow = Workflow(**self.workflow)
