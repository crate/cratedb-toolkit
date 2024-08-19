import pytest

pytestmark = pytest.mark.dynamodb


def test_dynamodb_import(dynamodb_test_manager):
    product_catalog = dynamodb_test_manager.load_product_catalog()
    assert product_catalog.item_count == 8
