import glob
import io
import logging
import os
import re
import tarfile
from typing import List

logger = logging.getLogger(__name__)


def archive_files(target_dir: str, exclude_patterns: List[str]) -> io.BytesIO:
    _partial = r")|(".join(exclude_patterns)
    pattern = rf"({_partial})"

    _bytes = io.BytesIO()
    with tarfile.open(mode="w:gz", fileobj=_bytes) as tar:
        for fn in glob.glob(os.path.join(target_dir, "**"), recursive=True):
            if not re.search(pattern, fn) and os.path.basename(fn) != "":
                logger.info(f"Added {fn} as {os.path.basename(fn)}")
                tar.add(fn, os.path.basename(fn))

    _bytes.seek(0)
    return _bytes
