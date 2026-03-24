from unittest import mock

import pytest

from cratedb_toolkit.io.router import IoRouter
from cratedb_toolkit.model import DatabaseAddress, InputOutputResource

pytest.importorskip("influxio", reason="Skipping router ILP tests because 'influxio' package is not installed")


CRATEDB_URL = "crate://localhost:4200/testdrive/demo"


@pytest.mark.parametrize(
    "source_url",
    [
        "observations.lp",
        "observations.lp.gz",
        "/tmp/export/data.lp",
        "/tmp/export/data.lp.gz",
        "https://example.com/data.lp",
        "http://example.com/data.lp.gz",
    ],
)
def test_router_load_lp_file_passthrough(source_url):
    # Bare .lp paths and HTTP .lp URLs must reach influxdb_copy unchanged.
    # The router dispatches on ``source_url.endswith(".lp")``, not the parsed scheme.
    with mock.patch("cratedb_toolkit.io.influxdb.influxdb_copy", return_value=True) as mock_copy:
        router = IoRouter()
        source = InputOutputResource(url=source_url)
        target = DatabaseAddress.from_string(CRATEDB_URL)
        result = router.load_table(source=source, target=target)
        assert result is True
        mock_copy.assert_called_once()
        actual_url = mock_copy.call_args[0][0]
        assert actual_url == source_url


@pytest.mark.parametrize(
    "source_url, expected_scheme",
    [
        ("influxdb2://example:token@localhost:8086/testdrive/demo", "http"),
        ("influxdb2://example:token@localhost:8086/testdrive/demo?ssl=true", "https"),
        ("influxdb://example:token@localhost:8086/testdrive/demo", "http"),
    ],
)
def test_router_load_influxdb_scheme_conversion(source_url, expected_scheme):
    # influxdb:// and influxdb2:// schemes must be converted to http(s):// for influxio.
    with mock.patch("cratedb_toolkit.io.influxdb.influxdb_copy", return_value=True) as mock_copy:
        router = IoRouter()
        source = InputOutputResource(url=source_url)
        target = DatabaseAddress.from_string(CRATEDB_URL)
        result = router.load_table(source=source, target=target)
        assert result is True
        mock_copy.assert_called_once()
        actual_url = mock_copy.call_args[0][0]
        assert actual_url.startswith(f"{expected_scheme}://")
        assert "localhost:8086" in actual_url
