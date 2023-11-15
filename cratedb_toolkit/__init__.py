try:
    from importlib.metadata import PackageNotFoundError, version
except (ImportError, ModuleNotFoundError):  # pragma:nocover
    from importlib_metadata import PackageNotFoundError, version  # type: ignore[assignment,no-redef,unused-ignore]

__appname__ = "cratedb-toolkit"

try:
    __version__ = version(__appname__)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"

from .api import ManagedCluster  # noqa: F401
from .config import configure  # noqa: F401
from .model import InputOutputResource, TableAddress  # noqa: F401
