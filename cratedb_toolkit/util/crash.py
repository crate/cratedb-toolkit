import contextlib
import os
import sys
import typing as t
from unittest import mock

from crate.crash.command import main

from cratedb_toolkit.util.client import jwt_token_patch


def run_crash(
    hosts: str,
    command: str,
    output_format: str = None,
    schema: str = None,
    username: str = None,
    password: str = None,
    jwt_token: str = None,
):
    """
    Run the interactive CrateDB database shell using `crash`.
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
    password_context: contextlib.AbstractContextManager = contextlib.nullcontext()
    if password:
        password_context = mock.patch.dict(os.environ, {"CRATEPW": password})
    with mock.patch.object(sys, "argv", cmd), jwt_token_patch(jwt_token=jwt_token), password_context:
        main()


def get_crash_output_formats() -> t.List[str]:
    """
    Inquire the output formats `crash` understands.
    """
    from crate.crash.outputs import OutputWriter
    from crate.crash.printer import PrintWrapper

    return OutputWriter(PrintWrapper(), False).formats
