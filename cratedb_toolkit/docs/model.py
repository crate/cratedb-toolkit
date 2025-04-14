import dataclasses
import logging

import requests

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class DocsItem:
    """
    Manage information about a documentation unit.
    """

    created: str
    generator: str
    source_url: str

    def fetch(self) -> str:
        response = requests.get(self.source_url, timeout=10)
        if response.status_code != 200:
            logger.error(
                f"Failed to fetch documentation from {self.source_url}: HTTP {response.status_code}\n{response.text}"
            )
            response.raise_for_status()
        return response.text
