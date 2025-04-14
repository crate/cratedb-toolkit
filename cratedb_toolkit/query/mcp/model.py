# ruff: noqa: T201
from __future__ import annotations

import asyncio
import dataclasses
import io
import logging
import os
import shlex
import textwrap
import typing as t
from contextlib import redirect_stdout

from cratedb_toolkit.query.mcp.util import to_json, to_yaml

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class McpServer:
    """
    Wrap information, installation and launch of an MCP server.
    """

    name: str
    command: str
    program: t.Optional[str] = None
    args: t.List[str] = dataclasses.field(default_factory=list)
    env: t.Dict[str, str] = dataclasses.field(default_factory=dict)
    requirements: t.List[str] = dataclasses.field(default_factory=list)
    preinstall: t.Optional[str] = None
    homepage: t.Optional[str] = None
    description: t.Optional[str] = None
    cratedb_validated: t.Optional[bool] = False
    example: t.Optional[str] = None
    issues: t.List[str] = dataclasses.field(default_factory=list)

    def __post_init__(self):
        """
        Split command and adjust description.
        """
        cmd = shlex.split(self.command)
        self.program = cmd[0]
        self.args = cmd[1:]
        self.description = self.description.strip() if self.description else None
        self.preinstall = self.preinstall.strip() if self.preinstall else None

    @property
    def install_command(self):
        """
        Return installation command for Python packages, using `uv`.
        """
        if self.requirements:
            requirements = [f"'{requirement}'" for requirement in self.requirements]
            return f"uv pip install {' '.join(requirements)}"
        return None

    def install(self):
        """
        Install MCP server, triggering both pre-install and main-install procedures.
        """
        if cmd := self.preinstall:
            os.system(cmd)  # noqa: S605
        if cmd := self.install_command:
            os.system(cmd)  # noqa: S605

    def launch(self):
        """
        Launch MCP server, currently in stdio mode only.

        TODO: Is it applicable to offer SSE mode here, using FastMCP?
        """
        from mcp import StdioServerParameters

        if self.program is None:
            raise ValueError("Program name for MCP server not defined")
        server_params = StdioServerParameters(
            command=self.program,
            args=self.args,
            env=self.env,
        )
        logger.info(f"Launching MCP server: {self.name}")
        logger.info(f"Command for MCP server '{self.name}': {self.command}")
        self._start_dummy(server_params)

    def _start_dummy(self, server_params):
        """
        Start server, just for dummy purposes.
        """
        loop = asyncio.new_event_loop()
        loop.create_task(self._launch_dummy(server_params))
        loop.run_forever()

    @staticmethod
    async def _launch_dummy(server_params):
        """
        Launch server, just for dummy purposes.

        FIXME: Currently, nobody can interact with this server:
               stdio is not forwarded, and SSE transport is not provided yet.
        """
        from mcp import stdio_client

        async with stdio_client(server_params) as (read, write):
            while True:
                await asyncio.sleep(1)

    def to_dict(self):
        return {
            "name": self.name,
            "description": self.description,
            "homepage": self.homepage,
            "install_command": self.install_command,
            "run": self.command,
            "preinstall": self.preinstall,
            "example": self.example and self.example.strip() or "",
        }

    def to_markdown(self):
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            print(f"## {self.name}")
            print()
            print(self.description)
            print()
            print(f":Homepage: <{self.homepage}>")
            print(f":Validated with CrateDB: {self.cratedb_validated}")
            if self.preinstall:
                print(":Preinstall:")
                print(textwrap.indent(f"```shell\n{self.preinstall.strip()}\n```", "  "))
            if self.install_command:
                print(f":Install: `{self.install_command}`")
            print(f":Run: `{self.command}`")
            if self.example:
                print(":Example:")
                print(textwrap.indent(f"```shell\n{self.example.strip()}\n```", "  "))
            print()
        return buffer.getvalue()

    def to_json(self):
        return to_json(self.to_dict())

    def to_yaml(self):
        return to_yaml(self.to_dict())
