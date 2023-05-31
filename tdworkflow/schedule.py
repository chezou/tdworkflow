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
        self.project = Project(**self.project)  # type: ignore
        self.workflow = Workflow(**self.workflow)  # type: ignore
        self.createdAt = parse_iso8601(self.createdAt)  # type: ignore
        self.updatedAt = parse_iso8601(self.updatedAt)  # type: ignore
        self.disabledAt = parse_iso8601(self.disabledAt)  # type: ignore
        self.nextRunTime = parse_iso8601(self.nextRunTime)  # type: ignore

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
        self.attempts = [Attempt(**att) for att in self.attempts]  # type: ignore
        self.project = Project(**self.project)  # type: ignore
        self.workflow = Project(**self.workflow)  # type: ignore
