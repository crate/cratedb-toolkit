import os
import sys
import typing as t

from crate.crash.command import main


def run_crash(
    hosts: str, command: str, output_format: str = None, schema: str = None, username: str = None, password: str = None
):
    """
    Run interactive CrateDB database shell, using `crash`.
    """

    cmd = ["crash", "--hosts", hosts]
    if username:
        cmd += ["--username", username]
    if schema:
        cmd += ["--schema", schema]
    if command:
        cmd += ["--command", command]
    if output_format:
        cmd += ["--format", output_format]
    sys.argv = cmd
    if password:
        os.environ["CRATEPW"] = password
    main()


def get_crash_output_formats() -> t.List[str]:
    """
    Inquire the output formats `crash` understands.
    """
    from crate.crash.outputs import OutputWriter
    from crate.crash.printer import PrintWrapper

    return OutputWriter(PrintWrapper(), False).formats
