import dataclasses


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
