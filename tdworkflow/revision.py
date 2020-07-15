import dataclasses
from datetime import datetime
from typing import Dict

from .resource import Resource
from .util import parse_iso8601


@dataclasses.dataclass
class Revision(Resource):
    revision: str
    createdAt: datetime = None
    archiveType: str = ""
    archiveMd5: str = ""
    userInfo: Dict = None

    def __post_init__(self):
        self.createdAt = parse_iso8601(self.createdAt)

    @property
    def archive_type(self):
        return self.archiveType

    @property
    def archive_md5(self):
        return self.archive_md5

    @property
    def created_at(self):
        return self.createdAt
