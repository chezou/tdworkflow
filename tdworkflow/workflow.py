import dataclasses
from datetime import datetime

from .project import Project
from .resource import Resource
from .util import parse_iso8601


@dataclasses.dataclass
class Workflow(Resource):
    id: int
    name: str
    project: Project = None
    timezone: str = ""
    config: dict = None
    revision: str = ""
    createdAt: datetime = None
    deletedAt: datetime = None
    updatedAt: datetime = None

    def __post_init__(self):
        self.id = int(self.id)
        self.project = Project(**self.project) if self.project else None
        self.createdAt = parse_iso8601(self.createdAt)
        self.deletedAt = parse_iso8601(self.deletedAt)
        self.updatedAt = parse_iso8601(self.updatedAt)

    @property
    def created_at(self):
        return self.createdAt

    @property
    def deleted_at(self):
        return self.deletedAt

    @property
    def updated_at(self):
        return self.updatedAt
