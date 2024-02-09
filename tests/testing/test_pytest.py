import re

import pytest


@pytest.mark.skip("Does not work, because something stalls when using multiple containers")
def test_cratedb_service(cratedb_service):
    """
    Verify the exported pytest fixture `cratedb_service` works as intended.
    """
    assert re.match(r"http://crate:@localhost:\d\d\d\d\d", cratedb_service.get_http_url())
    assert re.match(r"crate://crate:@localhost:\d\d\d\d\d", cratedb_service.get_connection_url())

    sql = "SELECT mountain FROM sys.summits ORDER BY prominence DESC LIMIT 1;"
    highest_summit = cratedb_service.database.run_sql(sql, records=True)[0]
    assert highest_summit["mountain"] == "Mont Blanc"
