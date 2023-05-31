import dataclasses
from datetime import datetime
from typing import Any, Dict, Optional

from .project import Project
from .resource import Resource
from .util import parse_iso8601


@dataclasses.dataclass
class Workflow(Resource):
    id: int
    name: str
    project: Optional[Project] = None
    timezone: str = ""
    config: Optional[Dict[str, Any]] = None
    revision: str = ""
    createdAt: Optional[datetime] = None
    deletedAt: Optional[datetime] = None
    updatedAt: Optional[datetime] = None

    def __post_init__(self) -> None:
        self.id = int(self.id)
        self.project = Project(**self.project) if self.project else None  # type: ignore
        self.createdAt = parse_iso8601(self.createdAt)  # type: ignore
        self.deletedAt = parse_iso8601(self.deletedAt)  # type: ignore
        self.updatedAt = parse_iso8601(self.updatedAt)  # type: ignore

    @property
    def created_at(self) -> Optional[datetime]:
        return self.createdAt

    @property
    def deleted_at(self) -> Optional[datetime]:
        return self.deletedAt

    @property
    def updated_at(self) -> Optional[datetime]:
        return self.updatedAt
