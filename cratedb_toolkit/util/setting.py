import dataclasses
import logging
import sys
import typing as t

import click
from dotenv import find_dotenv, load_dotenv

from cratedb_toolkit.config import CONFIG

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class Setting:
    click: click.Parameter
    group: t.Union[str, None] = None

    def asdict(self):
        return dataclasses.asdict(self)


def argv_has_long_option() -> bool:
    """
    Whether the command line contains any "long options" argument.
    """
    return any("--" in arg for arg in sys.argv[1:])


def init_dotenv():
    """
    Load environment variables from `.env` file.
    """
    if not CONFIG.RUNNING_ON_PYTEST:
        dotenv_file = find_dotenv()
        logger.info(f"Loading environment variables from .env file: {dotenv_file}")
        load_dotenv(dotenv_file)


def obtain_settings(specs: t.List[Setting], prog_name: str = None) -> t.Dict[str, str]:
    """
    Employ command-line parsing at runtime, using the `click` parser.

    Obtain configuration setting from different sources, DWIM-style.
    This is the generic workhorse utility variant.

    - Command line argument, in long form. Example: `--foobar=bazqux`.
    - Positional argument on command line. Example: `bazqux`.
    - Environment variable. Example: `export FOOBAR=bazqux`.
    - Environment variable prefix. Example: `export APPNAME_FOOBAR=bazqux`.
    """

    # Load environment variables from `.env` file.
    try:
        init_dotenv()
    except Exception as ex:
        logger.warning(f"Failed to load environment variables from .env file: {ex}")

    # Decode settings from command-line arguments or environment variables.
    prog_name = prog_name or sys.argv[0]
    click_specs = [spec.click for spec in specs]
    command = click.Command(prog_name, params=click_specs)
    if CONFIG.RUNNING_ON_JUPYTER or CONFIG.RUNNING_ON_PYTEST:
        args = []
    else:
        args = sys.argv[1:]
    try:
        with command.make_context(prog_name, args=args) as ctx:
            return ctx.params
    except click.exceptions.Exit as ex:
        if ex.exit_code != 0:
            raise
    return {}


def check_mutual_exclusiveness(
    specs: t.List[Setting], settings: t.Dict[str, str], message_none: str = None, message_multiple: str = None
):
    """
    Check settings for mutual exclusiveness.

    It has been inspired by click-option-group's RequiredMutuallyExclusiveOptionGroup.
    https://github.com/click-contrib/click-option-group
    """
    parameter_names = []
    environment_variables = []
    values = []
    for setting in specs:
        if setting.group is None:
            continue
        if setting.click.name is None:
            raise ValueError("Setting specification has no name")
        parameter_names.append(setting.click.opts[0])
        environment_variables.append(setting.click.envvar)
        value = settings.get(setting.click.name)
        values.append(value)
    guidance = f"Use one of the CLI argument {parameter_names} or environment variable {environment_variables}"
    if all(value is None for value in values):
        if message_none is None:
            message_none = f"One of the settings is required, but none of them have been specified. {guidance}"
        raise ValueError(message_none)
    if values.count(None) < len(values) - 1:
        if message_multiple is None:
            # Collect and format the specified settings.
            specified = [f"{param}={val}" for param, val in zip(parameter_names, values) if val is not None]
            message_multiple = (
                f"The settings are mutually exclusive, but multiple were specified: {', '.join(specified)}. {guidance}"
            )
        raise ValueError(message_multiple)
    return values
