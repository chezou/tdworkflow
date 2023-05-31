import dataclasses
from datetime import datetime
from typing import Any, Dict, Optional

from .resource import Resource
from .util import parse_iso8601


@dataclasses.dataclass
class LogFile(Resource):
    fileName: str
    taskName: str
    direct: Dict[Any, Any]
    fileSize: int
    agentId: str
    fileTime: Optional[datetime] = None

    def __post_init__(self) -> None:
        self.fileTime = parse_iso8601(self.fileTime)  # type: ignore

    @property
    def file_name(self) -> str:
        return self.fileName

    @property
    def taks_name(self) -> str:
        return self.taskName

    @property
    def file_time(self) -> Optional[datetime]:
        return self.fileTime

    @property
    def file_size(self) -> int:
        return self.fileSize

    @property
    def agent_id(self) -> str:
        return self.agentId
