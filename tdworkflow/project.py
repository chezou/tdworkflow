import dataclasses


@dataclasses.dataclass
class Project:
    id: int
    name: str
    revision: str = ""
    archiveType: str = ""
    archiveMd5: str = ""
    createdAt: str = ""
    deletedAt: str = ""
    updatedAt: str = ""

    def __post_init__(self):
        self.id = int(self.id)

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
