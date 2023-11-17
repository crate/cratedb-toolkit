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
        return "PYTEST_CURRENT_TEST" in os.environ


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


def configure_croud(no_spinner: bool = None, use_spinner: bool = None):
    """
    Turn off croud's Halo spinner when running in Jupyter Notebooks. It does not work well.

    - https://github.com/ManrajGrover/halo/issues/32
    - https://github.com/manrajgrover/halo/issues/179
    """
    if no_spinner or ((CONFIG.RUNNING_ON_JUPYTER or CONFIG.RUNNING_ON_PYTEST) and not use_spinner):
        mod = types.ModuleType(
            "croud.tools.spinner", "Mocking the croud.tools.spinner module, to turn off the Halo spinner"
        )
        setattr(mod, "HALO", NoopContextManager())  # noqa: B010
        sys.modules["croud.tools.spinner"] = mod


class NoopContextManager:
    """
    For making the Halo progressbar a no-op.
    """

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, exc_traceback):
        pass

    def stop(self):
        pass
