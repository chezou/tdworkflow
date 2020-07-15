import dataclasses
import json
from datetime import datetime
from typing import Dict, List

from .resource import Resource
from .util import parse_iso8601


@dataclasses.dataclass
class Task(Resource):
    id: int
    state: str
    updatedAt: datetime = None
    fullName: str = ""
    parentId: int = None
    upstreams: List[int] = None
    retryAt: datetime = None
    config: Dict = None
    exportParams: Dict = None
    storeParams: Dict = None
    stateParams: Dict = None
    error: Dict = None
    startedAt: datetime = None
    cancelRequested: bool = False
    isGroup: bool = False

    def __post_init__(self):
        self.id = int(self.id)
        self.updatedAt = parse_iso8601(self.updatedAt)
        self.parentId = int(self.parentId) if self.parentId else None
        self.upstreams = [int(_id) for _id in self.upstreams]
        self.retryAt = parse_iso8601(self.retryAt)
        self.startedAt = parse_iso8601(self.startedAt)

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
