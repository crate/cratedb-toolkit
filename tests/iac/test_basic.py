# ruff: noqa: F401
import pytest

pytestmark = pytest.mark.kinesis

pytest.importorskip("lorrystream", reason="Only works with LorryStream installed")


def test_iac_imports():
    from cratedb_toolkit.iac.aws import (
        DynamoDBKinesisPipe,
        LambdaFactory,
        LambdaPythonImage,
        RDSPostgreSQLDMSKinesisPipe,
    )
