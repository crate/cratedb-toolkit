from typing import Generator

import pytest

from cratedb_toolkit.testing.testcontainers.cratedb import CrateDBTestAdapter


@pytest.fixture(scope="session")
def cratedb_service() -> Generator[CrateDBTestAdapter, None, None]:
    """
    Provide a CrateDB service instance to the test suite.
    """
    db = CrateDBTestAdapter()
    db.start()
    yield db
    db.stop()
