import io
import logging
import os
import re
import tarfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional, Union

logger = logging.getLogger(__name__)


def archive_files(target_dir: str, exclude_patterns: List[str]) -> io.BytesIO:
    _partial = r")|(".join(exclude_patterns)
    pattern = rf"({_partial})"

    target_dir_path = Path(target_dir)
    _bytes = io.BytesIO()
    with tarfile.open(mode="w:gz", fileobj=_bytes) as tar:
        for current_dir, directories, files in os.walk(target_dir_path):
            for file_or_dir in [*directories, *files]:
                file_path = Path(os.path.join(current_dir, file_or_dir))
                if re.search(pattern, str(file_path)) or file_or_dir.startswith("."):
                    continue
                relative_path = file_path.relative_to(target_dir_path)
                logger.info(f"Added {file_path} as {relative_path}")
                tar.add(
                    file_path,
                    relative_path,
                    recursive=False,
                )

    _bytes.seek(0)
    return _bytes


def parse_iso8601(target: Optional[str]) -> Optional[datetime]:
    if not target:
        return None

    return datetime.fromisoformat(target.replace("Z", "+00:00"))


def to_iso8601(dt: Optional[Union[str, datetime]]) -> str:
    if not dt:
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
