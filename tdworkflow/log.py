import dataclasses
from datetime import datetime
from typing import Any

from .resource import Resource
from .util import parse_iso8601


@dataclasses.dataclass
class LogFile(Resource):
    fileName: str
    taskName: str
    direct: dict[Any, Any]
    fileSize: int
    agentId: str
    fileTime: datetime | None = None

    def __post_init__(self) -> None:
        if self.fileTime and isinstance(self.fileTime, str):
            self.fileTime = parse_iso8601(self.fileTime)

    @property
    def file_name(self) -> str:
        return self.fileName

    @property
    def taks_name(self) -> str:
        return self.taskName

    @property
    def file_time(self) -> datetime | None:
        return self.fileTime

    @property
    def file_size(self) -> int:
        return self.fileSize

    @property
    def agent_id(self) -> str:
        return self.agentId
