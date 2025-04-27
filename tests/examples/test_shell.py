import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent

pytestmark = pytest.mark.shell


def test_example_cloud_cluster_shell(cloud_environment):
    """
    Verify that the programs `examples/shell/{cloud_cluster.sh,cloud_import.sh}` succeed.
    """
    program = ROOT / "tests" / "examples" / "test_shell.sh"
    if not program.exists():
        pytest.fail(f"Test program not found: {program}")
    subprocess.check_call([str(program)])  # noqa: S603
