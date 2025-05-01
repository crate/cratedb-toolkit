import argparse
import contextlib
import dataclasses
import functools
import io
import json
import logging
import os
import typing as t
from pathlib import Path
from platform import python_version
from tempfile import NamedTemporaryFile
from unittest.mock import patch

import croud.api
import yaml
from boltons.typeutils import classproperty
from croud.config.configuration import Configuration

import cratedb_toolkit
from cratedb_toolkit.exception import CroudException

if t.TYPE_CHECKING:
    from croud.parser import Argument


logger = logging.getLogger(__name__)


@dataclasses.dataclass
class CroudCall:
    fun: t.Callable
    specs: t.List["Argument"]
    arguments: t.List[str]


class CroudWrapper:
    def __init__(self, call: CroudCall, output_format: str = None, decode_output: bool = True):
        """
        format: One of table,wide,json,yaml
        """
        output_format = output_format or "json"
        self.call = call
        self.output_format = output_format
        self.decode_output = decode_output

    def invoke_safedecode(self) -> t.Any:
        """
        TODO: Fix `croud clusters deploy`.
              It yields *two* payloads to stdout, making it
              unusable in JSON-capturing situations.

        FIXME: Remove after this patch has been merged and released:
               https://github.com/crate/croud/pull/564
        """
        # The main advantage of the `JSONDecoder` class is that it also provides
        # a `.raw_decode` method, which will ignore extra data after the end of the JSON.
        # https://stackoverflow.com/a/75168292
        payload = self.invoke()
        decoder = json.JSONDecoder()
        data, _ = decoder.raw_decode(payload)
        return data

    def invoke(self) -> t.Any:
        """
        Invoke croud function, and enrich response with explanation.
        """
        try:
            return self.invoke_real()
        except CroudException as ex:
            message = str(ex)
            logger.error(f"The operation failed: {ex}")
            explanation = None
            if "Unauthorized" in message:
                # FIXME: Complete error message.
                explanation = (
                    "Please make sure you logged in successfully with `croud login`, and `croud me` works well."
                )
            if "Resource not found" in message:
                explanation = (
                    "Apparently, the addressed cluster resource was not found. "
                    "Please validate the `--cluster-id` option, or the `CRATEDB_CLUSTER_ID` environment variable "
                    "used by CrateDB Toolkit."
                )
            if "Another cluster operation is currently in progress" in message:
                explanation = (
                    "The cluster deployment is probably still in progress. "
                    "Please wait until the cluster has been fully deployed."
                )
            if explanation:
                logger.warning(explanation)
            raise
        except Exception:
            raise

    @staticmethod
    def remove_version_argument(parser):
        for action in parser._actions:
            if "--version" in str(action):
                parser._remove_action(action)
        to_delete = []
        for key, action in parser._option_string_actions.items():
            if "--version" in str(action):
                to_delete.append(key)
        for delete in to_delete:
            del parser._option_string_actions[delete]

    def invoke_real(self) -> t.Any:
        """
        Invoke croud function, decode response from JSON, and return data.
        """
        from croud.parser import add_default_args, create_parser

        # Create argument parser.
        spec: t.Dict[str, t.Any] = {
            "help": None,
            "extra_args": [],
        }
        parser = create_parser(spec)
        self.remove_version_argument(parser)
        add_default_args(parser, omit=set())

        # Add command-specific arguments to parser.
        for arg_spec in self.call.specs:
            try:
                arg_spec.add_to_parser(parser)
            except argparse.ArgumentError:
                pass

        # Decode arguments.
        args_in = self.call.arguments + [f"--output-fmt={self.output_format}"]
        args = parser.parse_args(args_in)

        # Invoke croud function, with capturing.
        data = self.invoke_capturing(self.call.fun, args)

        if self.decode_output:
            if self.output_format == "json":
                return json.loads(data)
            elif self.output_format == "yaml":
                return yaml.safe_load(data)
        return data

    def invoke_capturing(self, fun: t.Callable, *args: t.List[t.Any], **kwargs: t.Dict[t.Any, t.Any]) -> str:
        """
        Invoke croud function, and return captured stdout.
        """
        croud_fun = functools.partial(fun, *args, **kwargs)
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            self.run_croud_fun(croud_fun)
        buffer.seek(0)
        return buffer.read()

    def run_croud_fun(self, fun: t.Callable, with_exceptions: bool = True):
        """
        Wrapper function to call into `croud`, for catching and converging error messages.

        The `croud` package, being conceived as a CLI program, uses `print` to output
        errors and other messages to stderr.

        This wrapper function catches them, emits them to the logger with an additional
        explanation, and eventually raises a `RuntimeException` on significant errors.
        That usually means that a CLI program will exit with an error code != 0.
        """

        # Central interceptor function for `print_*` functions.
        # Obtain level name as string as first positional argument.
        def print_fun(levelname: str, *args, **kwargs):
            level = get_sane_log_level(levelname)
            message = str(args[0])

            # Forward/propagate/emit log message from `croud`, or not.
            # Variant 1: Forward original log message 1:1.
            # logger.log(level, message)  # noqa: ERA001
            # Variant 2: Augment log message: Converge to `DEBUG` level.
            logger.log(logging.DEBUG, f"[croud] {levelname.upper():<8}: {message}")
            # TODO: Variant 3: Use setting in `EnvironmentConfiguration` to turn forwarding on/off.

            if with_exceptions and level >= logging.ERROR:
                raise CroudException(message)

        # https://stackoverflow.com/a/46481946
        levels = ["debug", "info", "warning", "error", "success"]
        with contextlib.ExitStack() as stack:
            # Patch all `print_*` functions, as they would obstruct the output.
            for level in levels:
                p = patch(f"croud.printer.print_{level}", functools.partial(print_fun, level))
                stack.enter_context(p)

            # Patch configuration.
            stack.enter_context(headless_config())

            # TODO: When aiming to disable wait-for-completion.
            """
            p = patch(f"croud.clusters.commands._wait_for_completed_operation")
            stack.enter_context(p)
            """

            # Invoke workhorse function.
            return fun()


@contextlib.contextmanager
def headless_config():
    """
    Patch the `croud.config` module to use a headless configuration.
    """
    with patch("croud.config._CONFIG", CroudClient.get_headless_config):
        yield


class CroudClient(croud.api.Client):
    """
    A slightly modified `croud.api.Client` class, to inject a custom User-Agent header.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        ua = f"{cratedb_toolkit.__appname__}/{cratedb_toolkit.__version__} Python/{python_version()}"
        self.session.headers["User-Agent"] = ua

    @staticmethod
    def create() -> "croud.api.Client":
        """
        Canonical factory method for creating a `croud.api.Client` instance.
        """
        with headless_config():
            from croud.config import CONFIG

            return croud.api.Client(
                CONFIG.endpoint,
                token=CONFIG.token,
                on_token=CONFIG.set_current_auth_token,
                key=CONFIG.key,
                secret=CONFIG.secret,
                region=CONFIG.region,
                sudo=False,
            )

    @classproperty
    def get_headless_config(cls) -> Configuration:
        cfg = Configuration("croud.yaml")
        if cfg._file_path.exists() and "CRATEDB_CLOUD_API_KEY" not in os.environ:
            return cfg

        tmp_file = NamedTemporaryFile()
        tmp_path = Path(tmp_file.name)
        config = Configuration("headless.yaml", tmp_path)

        # Get credentials from the environment.
        config.profile["key"] = os.environ.get("CRATEDB_CLOUD_API_KEY")
        config.profile["secret"] = os.environ.get("CRATEDB_CLOUD_API_SECRET")
        config.profile["organization-id"] = os.environ.get("CRATEDB_CLOUD_ORGANIZATION_ID")
        # config.profile["endpoint"] = os.environ.get("CRATEDB_CLOUD_ENDPOINT")  # noqa: ERA001

        return config


croud.api.Client = CroudClient


def get_sane_log_level(src) -> int:
    """
    https://stackoverflow.com/questions/18392804/python-logging-determine-level-number-from-name
    """
    level = logging.getLevelName(str(src).upper())
    if not isinstance(level, int):
        level = logging.NOTSET
    return level


def get_croud_output_formats() -> t.List[str]:
    """
    Inquire the output formats `croud` understands.
    """
    from croud.config.schemas import OUTPUT_FORMATS

    return OUTPUT_FORMATS


def table_fqn(table: str) -> str:
    """
    Return the table name, quoted, like `"<table>"`. When applicable,
    use the full qualified name `"<schema>"."<table>"`.

    Args:
        table: The table name or schema-qualified table name

    Returns:
        A properly quoted table name suitable for SQL queries

    Examples:
        >>> table_fqn("mytable")
        '"mytable"'
        >>> table_fqn("myschema.mytable")
        '"myschema"."mytable"'
        >>> table_fqn('"already.quoted"')
        '"already.quoted"'

    TODO: Possibly use more elaborate function from CTK.
    """

    # Empty or None table name.
    if not table:
        raise ValueError("Table name cannot be empty")

    if '"' in table:
        return table

    # Schema-qualified table name.
    if "." in table:
        # Handle multi-part qualified names (e.g., catalog.schema.table).
        parts = table.split(".")
        quoted_parts = [f'"{part}"' for part in parts]
        return ".".join(quoted_parts)

    # Simple table name
    else:
        return f'"{table}"'
