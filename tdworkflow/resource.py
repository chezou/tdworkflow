import dataclasses
import logging

logger = logging.getLogger(__name__)


class Resource:
    @classmethod
    def from_api_repr(cls, **resource):
        known_fields = {e.name for e in dataclasses.fields(cls)}
        original_values = {}
        for name in resource:
            if name in known_fields:
                original_values[name] = resource[name]
            else:
                logger.warning(f"'{name}' is unknown field. Ignored")

        return cls(**original_values)
