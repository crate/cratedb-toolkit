import dataclasses
from pathlib import Path

import requests


@dataclasses.dataclass
class ReleaseItem:
    """
    Manage information about a single release item.
    """

    category: str
    version: str
    date: str
    type: str
    url: str

    @property
    def cloud_version(self) -> str:
        """
        Get a release tag for the latest nightly release, suitable for use in CrateDB Cloud with the "nightly" channel.

        Example: nightly-6.0.0-2025-05-01-00-02
        """
        tag = Path(self.url).with_suffix("").stem
        if self.category == "nightly":
            tag = tag[:-8]
        return tag.replace("crate-", f"{self.category}-")


class CrateDBRelease:
    """
    Relay information about CrateDB releases.
    """

    URL = "https://cratedb.com/releases.json"

    def __init__(self):
        self.data = requests.get(self.URL, timeout=5).json()

    @property
    def stable(self):
        return self._release_item("stable")

    @property
    def testing(self):
        return self._release_item("testing")

    @property
    def nightly(self):
        return self._release_item("nightly")

    def _release_item(self, category: str):
        """
        Create a ReleaseItem instance for the given category.
        """
        slot = self.data[category]
        return ReleaseItem(
            category=category,
            version=slot["version"],
            date=slot["date"],
            type="tarball",
            url=slot["downloads"]["tar.gz"]["url"],
        )
