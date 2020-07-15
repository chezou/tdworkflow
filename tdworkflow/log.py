import dataclasses
from datetime import datetime
from typing import Dict

from .resource import Resource
from .util import parse_iso8601


@dataclasses.dataclass
class LogFile(Resource):
    fileName: str
    taskName: str
    direct: Dict
    fileSize: int
    agentId: str
    fileTime: datetime = None

    def __post_init__(self):
        self.fileTime = parse_iso8601(self.fileTime)

    @property
    def file_name(self):
        return self.fileName

    @property
    def taks_name(self):
        return self.taskName

    @property
    def file_time(self):
        return self.fileTime

    @property
    def file_size(self):
        return self.fileSize

    @property
    def agent_id(self):
        return self.agentId
