import pytest

from cratedb_toolkit.io.dynamodb.copy import DynamoDBFullLoad

pytestmark = pytest.mark.dynamodb


RECORD_UTM = {
    "Id": {"N": "101"},
    "utmTags": {
        "L": [
            {
                "M": {
                    "date": {"S": "2024-08-28T20:05:42.603Z"},
                    "utm_adgroup": {"L": [{"S": ""}, {"S": ""}]},
                    "utm_campaign": {"S": "34374686341"},
                    "utm_medium": {"S": "foobar"},
                    "utm_source": {"S": "google"},
                }
            }
        ]
    },
    "location": {
        "M": {
            "coordinates": {"L": [{"S": ""}]},
            "meetingPoint": {"S": "At the end of the tunnel"},
            "address": {"S": "Salzbergwerk Berchtesgaden"},
        },
    },
}


def test_dynamodb_copy(caplog, cratedb, dynamodb, dynamodb_test_manager):
    """
    CLI test: Invoke `ctk load table` for DynamoDB.
    """

    # Define source and target URLs.
    cratedb_url = f"{cratedb.get_connection_url()}/testdrive/demo"
    dynamodb_url = f"{dynamodb.get_connection_url()}/demo?region=us-east-1"

    # Populate source database with data.
    dynamodb_test_manager.load_records(table_name="demo", records=[RECORD_UTM])

    # Run transfer command.
    table_loader = DynamoDBFullLoad(dynamodb_url=dynamodb_url, cratedb_url=cratedb_url)
    table_loader.start()

    # Verify data in target database.
    assert cratedb.database.table_exists("testdrive.demo") is True
    assert cratedb.database.refresh_table("testdrive.demo") is True
    assert cratedb.database.count_records("testdrive.demo") == 1

    results = cratedb.database.run_sql("SELECT * FROM testdrive.demo;", records=True)  # noqa: S608
    assert results[0]["data"] == {
        "Id": 101.0,
        "utmTags": [
            {
                "date": "2024-08-28T20:05:42.603Z",
                "utm_adgroup": ["", ""],
                "utm_campaign": "34374686341",
                "utm_medium": "foobar",
                "utm_source": "google",
            }
        ],
        "location": {
            "coordinates": [""],
            "meetingPoint": "At the end of the tunnel",
            "address": "Salzbergwerk Berchtesgaden",
        },
    }
