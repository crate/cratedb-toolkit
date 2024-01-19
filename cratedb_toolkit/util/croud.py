import argparse
import contextlib
import dataclasses
import functools
import io
import json
import logging
import typing as t
from unittest.mock import patch

import yaml

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
                    "Please validate the `--cluster-id` option, or the `CRATEDB_CLOUD_CLUSTER_ID` environment variable "
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

    @staticmethod
    def run_croud_fun(fun: t.Callable, with_exceptions: bool = True):
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
            logger.log(level, message)
            if with_exceptions and level >= logging.ERROR:
                raise CroudException(message)

        # Patch all `print_*` functions, and invoke_foo workhorse function.
        # https://stackoverflow.com/a/46481946
        levels = ["debug", "info", "warning", "error", "success"]
        with contextlib.ExitStack() as stack:
            for level in levels:
                p = patch(f"croud.printer.print_{level}", functools.partial(print_fun, level))
                stack.enter_context(p)
            """
            TODO: When aiming to disable wait-for-completion.
                  p = patch(f"croud.clusters.commands._wait_for_completed_operation")
                  stack.enter_context(p)
            """
            return fun()


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
