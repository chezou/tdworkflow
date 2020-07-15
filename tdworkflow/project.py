import dataclasses
from datetime import datetime

from .resource import Resource
from .util import parse_iso8601


@dataclasses.dataclass
class Project(Resource):
    id: int
    name: str
    revision: str = ""
    archiveType: str = ""
    archiveMd5: str = ""
    createdAt: datetime = None
    deletedAt: datetime = None
    updatedAt: datetime = None

    def __post_init__(self):
        self.id = int(self.id)
        self.createdAt = parse_iso8601(self.createdAt)
        self.deletedAt = parse_iso8601(self.deletedAt)
        self.updatedAt = parse_iso8601(self.updatedAt)

    @property
    def archive_type(self):
        return self.archiveType

    @property
    def archive_md5(self):
        return self.archive_md5

    @property
    def created_at(self):
        return self.createdAt

    @property
    def deleted_at(self):
        return self.deletedAt

    @property
    def updated_at(self):
        return self.updatedAt
