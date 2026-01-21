import dataclasses
from datetime import datetime
from typing import Any

from .attempt import Attempt
from .project import Project
from .resource import Resource
from .util import parse_iso8601
from .workflow import Workflow

NextSchedule = dict[str, int | str | dict[str, Any]]


@dataclasses.dataclass
class Schedule(Resource):
    id: int
    project: Project
    workflow: Workflow
    createdAt: datetime | None = None
    updatedAt: datetime | None = None
    disabledAt: datetime | None = None
    nextScheduleTime: NextSchedule | None = None
    nextRunTime: datetime | None = None

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
    def created_at(self) -> datetime | None:
        return self.createdAt

    @property
    def updated_at(self) -> datetime | None:
        return self.updatedAt

    @property
    def disabled_at(self) -> datetime | None:
        return self.disabledAt

    @property
    def next_run_time(self) -> datetime | None:
        return self.nextRunTime

    @property
    def next_schedule_time(self) -> NextSchedule | None:
        return self.nextScheduleTime


@dataclasses.dataclass
class ScheduleAttempt(Resource):
    id: int
    attempts: list[Attempt]
    project: Project | None = None
    workflow: Workflow | None = None

    def __post_init__(self) -> None:
        self.id = int(self.id)
        self.attempts = [
            Attempt(**att) if isinstance(att, dict) else att for att in self.attempts
        ]
        if self.project and isinstance(self.project, dict):
            self.project = Project(**self.project)
        if self.workflow and isinstance(self.workflow, dict):
            self.workflow = Workflow(**self.workflow)
