import glob
import io
import logging
import os
import re
import tarfile
from datetime import datetime, timedelta, timezone
from typing import List, Optional

logger = logging.getLogger(__name__)


def exclude_files(pattern):
    def _filter(tarinfo):
        if re.search(pattern, tarinfo.name) or os.path.basename(
            tarinfo.name
        ).startswith("."):
            return None
        else:
            return tarinfo

    return _filter


def archive_files(target_dir: str, exclude_patterns: List[str]) -> io.BytesIO:
    _partial = r")|(".join(exclude_patterns)
    pattern = rf"({_partial})"

    _bytes = io.BytesIO()
    with tarfile.open(mode="w:gz", fileobj=_bytes) as tar:
        for fn in glob.glob(os.path.join(target_dir, "**"), recursive=True):
            if not re.search(pattern, fn) and os.path.basename(fn) != "":
                write_fn = fn.replace(f"{target_dir}/", "")
                logger.info(f"Added {fn} as {write_fn}")
                tar.add(fn, write_fn, recursive=False)

    _bytes.seek(0)
    return _bytes


def parse_iso8601(target: str) -> Optional[datetime]:
    if not target:
        return None

    return datetime.fromisoformat(target.replace("Z", "+00:00"))


def to_iso8601(dt: datetime) -> str:
    if not datetime:
        return ""

    if isinstance(dt, datetime):
        # Naive object
        if not dt.tzinfo:
            return dt.astimezone(timezone(timedelta(0), "UTC")).isoformat()
        # Aware object
        else:
            return dt.isoformat()

    elif isinstance(dt, str):
        return dt

    else:
        raise ValueError("Unexpected type")
