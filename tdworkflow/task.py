import dataclasses
import json
from typing import Dict, List


@dataclasses.dataclass
class Task:
    id: int
    state: str
    updatedAt: str = ""
    fullName: str = ""
    parentId: int = None
    upstreams: List[int] = None
    retryAt: str = None
    config: Dict = None
    exportParams: Dict = None
    storeParams: Dict = None
    stateParams: Dict = None
    error: Dict = None
    startedAt: str = None
    cancelRequested: bool = False
    isGroup: bool = False

    def __post_init__(self):
        self.id = int(self.id)
        self.parentId = int(self.parentId) if self.parentId else None
        self.upstreams = [int(_id) for _id in self.upstreams]

    @property
    def updated_at(self):
        return self.updatedAt

    @property
    def full_name(self):
        return self.fullName

    @property
    def parent_id(self):
        return self.parentId

    @property
    def retry_at(self):
        return self.retryAt

    @property
    def export_params(self):
        return self.exportParams

    @property
    def store_params(self):
        return self.storeParams

    @property
    def state_params(self):
        return self.stateParams

    @property
    def started_at(self):
        return self.startedAt

    @property
    def cancel_requested(self):
        return self.cancelRequested

    @property
    def group(self):
        return self.isGroup


class TaskEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Task):
            return obj.__dict__
        return json.JSONEncoder.default(self, obj)
