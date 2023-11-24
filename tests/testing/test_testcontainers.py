import pytest

from cratedb_toolkit.testing.testcontainers.cratedb import CrateDBContainer


@pytest.mark.parametrize(
    "opts, expected",
    [
        pytest.param(
            {"indices.breaker.total.limit": "90%"},
            (
                "-Cdiscovery.type=single-node "
                "-Cnode.attr.storage=hot "
                "-Cpath.repo=/tmp/snapshots "
                "-Cindices.breaker.total.limit=90%"
            ),
            id="add_cmd_option",
        ),
        pytest.param(
            {"discovery.type": "zen", "indices.breaker.total.limit": "90%"},
            (
                "-Cdiscovery.type=zen "
                "-Cnode.attr.storage=hot "
                "-Cpath.repo=/tmp/snapshots "
                "-Cindices.breaker.total.limit=90%"
            ),
            id="override_defaults",
        ),
    ],
)
def test_build_command(opts, expected):
    db = CrateDBContainer(cmd_opts=opts)
    assert db._command == expected
