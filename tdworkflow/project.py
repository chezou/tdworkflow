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
        self.createdAt = parse_iso8601(self.createdAt)  # type: ignore
        self.deletedAt = parse_iso8601(self.deletedAt)  # type: ignore
        self.updatedAt = parse_iso8601(self.updatedAt)  # type: ignore

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
