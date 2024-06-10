from typing import Generator

import pytest

try:
    from cratedb_toolkit.testing.testcontainers.cratedb import CrateDBTestAdapter

    @pytest.fixture(scope="session")
    def cratedb_service() -> Generator[CrateDBTestAdapter, None, None]:
        """
        Provide a CrateDB service instance to the test suite.
        """
        db = CrateDBTestAdapter(crate_version="nightly")
        db.start()
        yield db
        db.stop()

except ModuleNotFoundError as ex:
    message = str(ex)

    @pytest.fixture(scope="session")
    def cratedb_service():
        """
        An error handling surrogate used when dependencies are not satisfied.
        In order to use it, invoke `pip install 'cratedb-toolkit[testing]'`.
        """
        raise NotImplementedError(message)
