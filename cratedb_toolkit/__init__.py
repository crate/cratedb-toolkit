# ruff: noqa: E402
try:
    from importlib.metadata import PackageNotFoundError, version
except (ImportError, ModuleNotFoundError):  # pragma:nocover
    from importlib_metadata import PackageNotFoundError, version  # type: ignore[assignment,no-redef,unused-ignore]

__appname__ = "cratedb-toolkit"

try:
    __version__ = version(__appname__)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"

from .config import preconfigure

preconfigure()

from .cluster.core import DatabaseCluster, ManagedCluster, StandaloneCluster
from .config import configure
from .model import InputOutputResource, TableAddress

__all__ = [
    "ManagedCluster",
    "DatabaseCluster",
    "StandaloneCluster",
    "InputOutputResource",
    "TableAddress",
    "configure",
]
