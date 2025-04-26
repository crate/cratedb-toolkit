import pytest

from cratedb_toolkit.cmd.tail.main import TableTailer
from cratedb_toolkit.model import TableAddress


def test_tail_sys_summits_default(cratedb):
    tt = TableTailer(db=cratedb.database, resource=TableAddress.from_string("sys.summits"))
    results = tt.start(lines=42)
    assert len(results) == 42


def test_tail_sys_summits_format_log(cratedb):
    tt = TableTailer(db=cratedb.database, resource=TableAddress.from_string("sys.summits"), format="log")
    with pytest.raises(NotImplementedError) as ex:
        tt.start(lines=2)
    assert ex.match("Log output only implemented for `sys.jobs_log`.*")


def test_tail_sys_summits_format_yaml(cratedb):
    tt = TableTailer(db=cratedb.database, resource=TableAddress.from_string("sys.summits"), format="yaml")
    results = tt.start(lines=2)
    assert len(results) == 2


def test_tail_sys_jobs_log_default(cratedb):
    tt = TableTailer(db=cratedb.database, resource=TableAddress.from_string("sys.jobs_log"))
    results = tt.start(lines=2)
    assert len(results) >= 1


def test_tail_sys_jobs_log_format_json(cratedb):
    tt = TableTailer(db=cratedb.database, resource=TableAddress.from_string("sys.jobs_log"), format="json")
    results = tt.start(lines=2)
    assert len(results) >= 1
