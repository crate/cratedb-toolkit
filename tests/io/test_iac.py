# ruff: noqa: F401


def test_iac_imports():
    from cratedb_toolkit.iac.aws import (
        DynamoDBKinesisPipe,
        LambdaFactory,
        LambdaPythonImage,
        RDSPostgreSQLDMSKinesisPipe,
    )
