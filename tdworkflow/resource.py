import dataclasses
import logging
from typing import Any, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T", bound="Resource")


class Resource:
    @classmethod
    def from_api_repr(cls: type[T], **resource: Any) -> T:
        # https://github.com/python/mypy/issues/14941
        known_fields = {e.name for e in dataclasses.fields(cls)}  # type: ignore
        original_values = {}
        for name in resource:
            if name in known_fields:
                original_values[name] = resource[name]
            else:
                logger.warning(f"'{name}' is unknown field. Ignored")

        return cls(**original_values)
