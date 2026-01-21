import dataclasses
from datetime import datetime
from typing import Any

from .project import Project
from .resource import Resource
from .util import parse_iso8601


@dataclasses.dataclass
class Workflow(Resource):
    id: int
    name: str
    project: Project | None = None
    timezone: str = ""
    config: dict[str, Any] | None = None
    revision: str = ""
    createdAt: datetime | None = None
    deletedAt: datetime | None = None
    updatedAt: datetime | None = None

    def __post_init__(self) -> None:
        self.id = int(self.id)
        if self.project and isinstance(self.project, dict):
            self.project = Project(**self.project)
        if self.createdAt and isinstance(self.createdAt, str):
            self.createdAt = parse_iso8601(self.createdAt)
        if self.deletedAt and isinstance(self.deletedAt, str):
            self.deletedAt = parse_iso8601(self.deletedAt)
        if self.updatedAt and isinstance(self.updatedAt, str):
            self.updatedAt = parse_iso8601(self.updatedAt)

    @property
    def created_at(self) -> datetime | None:
        return self.createdAt

    @property
    def deleted_at(self) -> datetime | None:
        return self.deletedAt

    @property
    def updated_at(self) -> datetime | None:
        return self.updatedAt
