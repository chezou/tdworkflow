import dataclasses
from datetime import datetime
from typing import Any, Dict, Optional

from .resource import Resource
from .util import parse_iso8601


@dataclasses.dataclass
class Revision(Resource):
    revision: str
    createdAt: Optional[datetime] = None
    archiveType: str = ""
    archiveMd5: str = ""
    userInfo: Optional[Dict[str, Any]] = None

    def __post_init__(self) -> None:
        if self.createdAt and isinstance(self.createdAt, str):
            self.createdAt = parse_iso8601(self.createdAt)

    @property
    def archive_type(self) -> str:
        return self.archiveType

    @property
    def archive_md5(self) -> str:
        return self.archive_md5

    @property
    def created_at(self) -> Optional[datetime]:
        return self.createdAt
