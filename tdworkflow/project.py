import dataclasses
from datetime import datetime
from typing import Optional

from .resource import Resource
from .util import parse_iso8601


@dataclasses.dataclass
class Project(Resource):
    id: int
    name: str
    revision: str = ""
    archiveType: str = ""
    archiveMd5: str = ""
    createdAt: Optional[datetime] = None
    deletedAt: Optional[datetime] = None
    updatedAt: Optional[datetime] = None

    def __post_init__(self) -> None:
        self.id = int(self.id)
        if self.createdAt and isinstance(self.createdAt, str):
            self.createdAt = parse_iso8601(self.createdAt)
        if self.deletedAt and isinstance(self.deletedAt, str):
            self.deletedAt = parse_iso8601(self.deletedAt)
        if self.updatedAt and isinstance(self.updatedAt, str):
            self.updatedAt = parse_iso8601(self.updatedAt)

    @property
    def archive_type(self) -> str:
        return self.archiveType

    @property
    def archive_md5(self) -> str:
        return self.archive_md5

    @property
    def created_at(self) -> Optional[datetime]:
        return self.createdAt

    @property
    def deleted_at(self) -> Optional[datetime]:
        return self.deletedAt

    @property
    def updated_at(self) -> Optional[datetime]:
        return self.updatedAt
