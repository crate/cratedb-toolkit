import functools
import logging
import sys
import typing as t

from cratedb_toolkit.config import CONFIG

logger = logging.getLogger()


def flexfun(domain: t.Literal["runtime", "settings"] = None):
    """
    Function decorator, which honors toolkit environment settings wrt. error handling.

    It is sorting out whether to raise exceptions, or whether to just `exit({1,2})`.

    This detail is important to handle well depending on the runtime environment. It can
    either be a standalone program, used on behalf of a library, or within a Jupyter
    Notebook.

    Regarding the exit code, let's just select one of 1 or 2.
    https://www.gnu.org/software/wget/manual/html_node/Exit-Status.html

    0

        No problems occurred.
    1

        Generic error code.
    2

        Parse error. For instance, when parsing command-line options or config files.

    -- https://www.pythontutorial.net/advanced-python/python-decorator-arguments/
    """

    runtime_error_exit_code = 1
    settings_error_exit_code = 2

    def decorate(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Exception as ex:
                if domain == "runtime":
                    runtime_mode = getattr(CONFIG, "runtime_errors", "raise")
                    if runtime_mode == "raise":
                        raise
                    elif runtime_mode == "exit":  # noqa: RET506
                        logger.error(ex)
                        sys.exit(runtime_error_exit_code)
                    else:
                        raise NotImplementedError(
                            f"Unknown way to handle settings errors: {CONFIG.runtime_errors}"
                        ) from ex
                elif domain == "settings":
                    settings_mode = getattr(CONFIG, "settings_errors", "raise")
                    if settings_mode == "raise":
                        raise
                    elif settings_mode == "exit":  # noqa: RET506
                        logger.error(ex)
                        sys.exit(settings_error_exit_code)
                    else:
                        raise NotImplementedError(
                            f"Unknown way to handle settings errors: {CONFIG.settings_errors}"
                        ) from ex
                else:
                    logger.debug(f"Not suppressing exception on unknown domain: {domain}")
                    raise

        return wrapper

    return decorate
