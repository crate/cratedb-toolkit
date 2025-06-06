import contextlib
import dataclasses
import os
import sys
import types


@dataclasses.dataclass
class EnvironmentConfiguration:
    """
    Manage information about the toolkit environment.
    """

    frontend_interactive: bool = True
    frontend_guidance: bool = True
    runtime_errors: str = "raise"
    settings_accept_cli: bool = False
    settings_accept_env: bool = False
    settings_errors: str = "raise"

    @property
    def RUNNING_ON_JUPYTER(self):
        return "JPY_SESSION_NAME" in os.environ

    @property
    def RUNNING_ON_PYTEST(self):
        return "PYTEST_CURRENT_TEST" in os.environ or "PYTEST_VERSION" in os.environ


# The global toolkit environment.
# TODO: Provide as context manager.
CONFIG = EnvironmentConfiguration()


def configure(
    frontend_interactive: bool = True,
    frontend_guidance: bool = True,
    runtime_errors: str = "raise",
    settings_accept_cli: bool = False,
    settings_accept_env: bool = False,
    settings_errors: str = "raise",
):
    """
    Configure the toolkit environment.
    """
    CONFIG.frontend_interactive = frontend_interactive
    CONFIG.frontend_guidance = frontend_guidance
    CONFIG.runtime_errors = runtime_errors
    CONFIG.settings_accept_cli = settings_accept_cli
    CONFIG.settings_accept_env = settings_accept_env
    CONFIG.settings_errors = settings_errors


def preconfigure():
    """
    Configure a few environment details very early.
    """
    configure_croud()

    # When running on Jupyter Notebooks, accept environment variables only.
    if CONFIG.RUNNING_ON_JUPYTER or CONFIG.RUNNING_ON_PYTEST:
        configure(
            settings_accept_cli=False,
            settings_accept_env=True,
        )


def configure_croud(no_spinner: bool = False, use_spinner: bool = False):
    """
    FIXME: Turn off croud's Halo spinner when running in Jupyter Notebooks, it does not work well.

    Issue:
    - https://github.com/crate/croud/issues/567

    Related issues:
    - https://github.com/ManrajGrover/halo/issues/32
    - https://github.com/manrajgrover/halo/issues/179
    """

    # Make the Halo progressbar a no-op in certain conditions.
    if no_spinner or ((CONFIG.RUNNING_ON_JUPYTER or CONFIG.RUNNING_ON_PYTEST) and not use_spinner):
        mod = types.ModuleType(
            "croud.tools.spinner", "Mocking the croud.tools.spinner module, to turn off the Halo spinner"
        )
        setattr(mod, "HALO", NoopHalo())  # noqa: B010
        sys.modules["croud.tools.spinner"] = mod


class NoopHalo(contextlib.nullcontext):
    """
    For making the Halo progressbar a no-op.
    """

    def stop(self):
        self.__exit__(None, None, None)
