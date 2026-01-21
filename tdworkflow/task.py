import dataclasses
import json
from datetime import datetime
from typing import Any

from .resource import Resource
from .util import parse_iso8601


@dataclasses.dataclass
class Task(Resource):
    id: int
    state: str
    updatedAt: datetime | None = None
    fullName: str = ""
    parentId: int | None = None
    upstreams: list[int] | None = None
    retryAt: datetime | None = None
    config: dict[str, Any] | None = None
    exportParams: dict[str, Any] | None = None
    storeParams: dict[str, Any] | None = None
    stateParams: dict[str, Any] | None = None
    error: dict[str, Any] | None = None
    startedAt: datetime | None = None
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
    def updated_at(self) -> datetime | None:
        return self.updatedAt

    @property
    def full_name(self) -> str:
        return self.fullName

    @property
    def parent_id(self) -> int | None:
        return self.parentId

    @property
    def retry_at(self) -> datetime | None:
        return self.retryAt

    @property
    def export_params(self) -> dict[str, Any] | None:
        return self.exportParams

    @property
    def store_params(self) -> dict[str, Any] | None:
        return self.storeParams

    @property
    def state_params(self) -> dict[str, Any] | None:
        return self.stateParams

    @property
    def started_at(self) -> datetime | None:
        return self.startedAt

    @property
    def cancel_requested(self) -> bool:
        return self.cancelRequested

    @property
    def group(self) -> bool:
        return self.isGroup


class TaskEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, Task):
            return o.__dict__
        return json.JSONEncoder.default(self, o)
