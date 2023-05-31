import dataclasses
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from .resource import Resource
from .util import parse_iso8601


@dataclasses.dataclass
class Task(Resource):
    id: int
    state: str
    updatedAt: Optional[datetime] = None
    fullName: str = ""
    parentId: Optional[int] = None
    upstreams: Optional[List[int]] = None
    retryAt: Optional[datetime] = None
    config: Optional[Dict[str, Any]] = None
    exportParams: Optional[Dict[str, Any]] = None
    storeParams: Optional[Dict[str, Any]] = None
    stateParams: Optional[Dict[str, Any]] = None
    error: Optional[Dict[str, Any]] = None
    startedAt: Optional[datetime] = None
    cancelRequested: bool = False
    isGroup: bool = False

    def __post_init__(self) -> None:
        self.id = int(self.id)
        if self.updatedAt and isinstance(self.updatedAt, str):
            self.updatedAt = parse_iso8601(self.updatedAt)
        self.parentId = int(self.parentId) if self.parentId else None
        self.upstreams = [int(_id) for _id in self.upstreams] if self.upstreams else []
        if self.retryAt and isinstance(self.retryAt, str):
            self.retryAt = parse_iso8601(self.retryAt)
        if self.startedAt and isinstance(self.startedAt, str):
            self.startedAt = parse_iso8601(self.startedAt)

    @property
    def updated_at(self) -> Optional[datetime]:
        return self.updatedAt

    @property
    def full_name(self) -> str:
        return self.fullName

    @property
    def parent_id(self) -> Optional[int]:
        return self.parentId

    @property
    def retry_at(self) -> Optional[datetime]:
        return self.retryAt

    @property
    def export_params(self) -> Optional[Dict[str, Any]]:
        return self.exportParams

    @property
    def store_params(self) -> Optional[Dict[str, Any]]:
        return self.storeParams

    @property
    def state_params(self) -> Optional[Dict[str, Any]]:
        return self.stateParams

    @property
    def started_at(self) -> Optional[datetime]:
        return self.startedAt

    @property
    def cancel_requested(self) -> bool:
        return self.cancelRequested

    @property
    def group(self) -> bool:
        return self.isGroup


class TaskEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, Task):
            return obj.__dict__
        return json.JSONEncoder.default(self, obj)
