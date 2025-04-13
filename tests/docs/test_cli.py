import json
from pathlib import Path

from click.testing import CliRunner

from cratedb_toolkit.docs.cli import cli


def test_settings():
    """
    Verify `ctk docs settings`.
    """
    # Path to the output file.
    output = Path("cratedb_settings.json")

    # Clean up any existing file before the test.
    output.unlink(missing_ok=True)

    # Invoke command.
    runner = CliRunner()
    result = runner.invoke(
        cli,
        args="settings",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify the outcome.
    assert output.exists(), f"File missing: {output}"
    data = json.loads(output.read_text())
    assert "whether or not to collect statistical information" in data["stats.enabled"]["purpose"]

    # Clean up after the test
    if output.exists():
        output.unlink()
