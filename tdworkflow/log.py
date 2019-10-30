import dataclasses
from typing import Dict


@dataclasses.dataclass
class LogFile:
    fileName: str
    taskName: str
    fileTime: int
    direct: Dict
    fileSize: int
    agentId: str

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
