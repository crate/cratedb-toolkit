import pytest

pytest.importorskip("boto3", reason="Skipping DynamoDB tests because 'boto3' package is not installed")
