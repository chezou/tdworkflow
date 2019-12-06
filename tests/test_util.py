import tarfile

from tdworkflow.util import archive_files


def test_archive_files():
    expected_files = ["py_scripts", "py_scripts/exec.py", "main.dig"]

    target_dir = "tests/resources/sample_project"
    excludes = ["ignore_dir", "__ignoredir__"]
    data = archive_files(target_dir, excludes)

    files = []
    with tarfile.open(mode="r:gz", fileobj=data) as tar:
        files = [t.name for t in tar]

    assert len(expected_files) == len(files)
    assert sorted(expected_files) == sorted(files)
