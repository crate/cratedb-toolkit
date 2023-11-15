import argparse
import dataclasses
import logging
import os
import sys
import typing as t

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class Setting:
    name: str
    envvar: str
    help: str  # noqa: A003

    def asdict(self):
        return dataclasses.asdict(self)


def obtain_setting(
    pos: int = None,
    name: str = None,
    default: t.Any = None,
    help: str = None,  # noqa: A002
    envvar: str = None,
    envprefix: str = None,
    errors: str = "raise",
    error_message: str = None,
    use_dotenv: bool = True,
) -> t.Optional[str]:
    """
    Obtain configuration setting from different sources, DWIM-style.
    This is the generic workhorse utility variant.

    - Command line argument, in long form. Example: `--foobar=bazqux`.
    - Positional argument on command line. Example: `bazqux`.
    - Environment variable. Example: `export FOOBAR=bazqux`.
    - Environment variable prefix. Example: `export APPNAME_FOOBAR=bazqux`.
    """

    # 0. Specials.
    # When no env_name is given, but an env_prefix *and* a CLI arg name are given,
    # make up an environment variable name like `--foo` => `APP_FOO`.
    if envvar is None and envprefix is not None and name:
        if not isinstance(envprefix, str) or not isinstance(name, str):
            raise TypeError("Wrong type for arguments `env_prefix` or `cli_argument_name`")
        envvar = envprefix.upper() + name.strip("-").replace("-", "_").upper()

    # Optionally collect an error message about the decoding step.
    decoding_error = None

    # 1. Decode named argument.
    if name is not None:
        parser, value = obtain_setting_cli(name=name, default=default, help=help)
        if value is not None:
            return value

    # 2. Decode positional argument.
    if pos is not None and not argv_has_long_option():
        try:
            return sys.argv[pos]
        except IndexError as ex:
            decoding_error = f"{ex.__class__.__name__}: {ex}"

    # 3. Decode environment variable.
    if envvar is not None:
        if use_dotenv:
            from dotenv import find_dotenv, load_dotenv

            load_dotenv(find_dotenv())
        try:
            return os.environ[envvar]
        except KeyError as ex:
            decoding_error = f"{ex.__class__.__name__}: {ex}"

    # 4. Prepare error reporting.
    if error_message is None:
        error_message = (
            f"Unable to obtain configuration setting from "
            f"command line argument `{name}`, at "
            f"argv position `{pos}`, or through the "
            f"environment variable `{envvar}`: {decoding_error}"
        )

    # 5. Report about decoding errors, or propagate them as exception.
    if errors != "ignore":
        logger.warning(error_message)

        # When the ArgumentParser is involved, use its help message.
        if name:
            logger.warning(f"Unable to decode command line options, see usage information:\n{parser.format_help()}")

    # By default, raise an exception when no value could be obtained.
    if errors == "raise":  # noqa: RET503
        raise ValueError(error_message)

    return None


def obtain_setting_cli(
    name: str, default: str = None, help: str = None  # noqa: A002
) -> t.Tuple[argparse.ArgumentParser, t.Optional[str]]:
    """
    Obtain a command line argument value from `sys.argv`.
    """
    parser = argparse.ArgumentParser()
    arg = parser.add_argument(name, default=default, help=help)
    try:
        namespace, args = parser.parse_known_args()
        return parser, getattr(namespace, arg.dest)
    except argparse.ArgumentError:
        return parser, None


def argv_has_long_option() -> bool:
    """
    Whether the command line contains any "long options" argument.
    """
    return any("--" in arg for arg in sys.argv[1:])


class RequiredMutuallyExclusiveSettingsGroup:
    """
    Obtain configuration settings from different sources, DWIM-style.
    This variant provides grouping, in terms of mutual exclusiveness of multiple settings.

    It has been inspired by click-option-group's RequiredMutuallyExclusiveOptionGroup.
    https://github.com/click-contrib/click-option-group
    """

    def __init__(self, *settings, message_none: str = None, message_multiple: str = None):
        self.settings = settings
        self.message_multiple = message_multiple
        self.message_none = message_none

    def obtain_settings(self):
        names = []
        envvars = []
        values = []
        for setting in self.settings:
            names.append(setting.name)
            envvars.append(setting.envvar)
            value = obtain_setting(**setting.asdict(), errors="ignore")
            values.append(value)
        guidance = f"Use one of the CLI argument {names} or environment variable {envvars}"
        if all(value is None for value in values):
            message = self.message_none
            if message is None:
                message = f"One of the settings is required, but none of them have been specified. {guidance}"
            raise ValueError(message)
        if values.count(None) < len(self.settings) - 1:
            message = self.message_multiple
            if message is None:
                message = f"The settings are mutually exclusive, but multiple of them have been specified. {guidance}"
            raise ValueError(message)
        return values
