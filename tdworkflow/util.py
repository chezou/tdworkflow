import io
import logging
import os
import re
import tarfile
from typing import List

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
        tar.add(target_dir, arcname="./", filter=exclude_files(pattern))

    _bytes.seek(0)
    return _bytes
