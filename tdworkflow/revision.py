import dataclasses
from typing import Dict


@dataclasses.dataclass
class Revision:
    revision: str
    createdAt: str = ""
    archiveType: str = ""
    archiveMd5: str = ""
    userInfo: Dict = None

    @property
    def archive_type(self):
        return self.archiveType

    @property
    def archive_md5(self):
        return self.archive_md5

    @property
    def created_at(self):
        return self.createdAt
