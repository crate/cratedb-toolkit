"""
Unit tests for the information elements and containers.

`ctk info jobs` and friends are assembled from `InfoElement` instances, and the element
names are the keys of the emitted JSON document.
"""

import io
import typing as t
from pathlib import Path

import pytest

from cratedb_toolkit.info.core import InfoContainer, JobInfoContainer, LogContainer
from cratedb_toolkit.info.library import Library
from cratedb_toolkit.info.model import ElementStore, InfoContainerBase, InfoElement, LogElement
from cratedb_toolkit.info.util import get_single_value
from cratedb_toolkit.util.database import DatabaseAdapter

# The output keys of `ctk info jobs`. Changing them breaks consumers of the JSON document.
JOB_ELEMENT_NAMES = [
    "age_range",
    "by_user",
    "duration_buckets",
    "duration_percentiles",
    "history",
    "history_count",
    "performance15min",
    "running",
    "running_count",
    "top100_count",
    "top100_duration_individual",
    "top100_duration_total",
]


class FakeAdapter(DatabaseAdapter):
    """
    Record SQL statements instead of running them, and reply with a canned response.

    Deliberately does not invoke `DatabaseAdapter.__init__` to not connect DB.
    """

    def __init__(self, response=None):
        self.response = response if response is not None else [{"job_count": 42}]
        self.queries: t.List[str] = []

    def run_sql(
        self,
        sql: t.Union[str, Path, io.IOBase],
        parameters: t.Optional[t.Mapping[str, str]] = None,
        records: bool = False,
        ignore: t.Optional[str] = None,
    ):
        self.queries.append(str(sql))
        return self.response


def test_job_info_container_elements():
    """
    Verify `ctk info jobs` emits exactly the well-known set of elements.
    """
    
    container = JobInfoContainer(adapter=FakeAdapter())
    assert sorted(container.elements.index) == JOB_ELEMENT_NAMES
    assert len(container.elements.items) == len(JOB_ELEMENT_NAMES)


def test_job_info_element_names_differ_from_attributes():
    """
    Verify the `history100` attribute is emitted as `history`.
    """

    assert Library.JobInfo.history100.name == "history"


def test_job_info_elements_are_documented():
    """
    Verify each job element carries a label and a description, for the docs to stay truthful.
    """
    container = JobInfoContainer(adapter=FakeAdapter())
    for element in container.elements.items:
        assert element.label, f"Element without label: {element.name}"
        assert element.description, f"Element without description: {element.name}"
        assert element.sql.strip(), f"Element without SQL: {element.name}"


@pytest.mark.parametrize(
    ("container_factory", "count"),
    [(InfoContainer, 18), (JobInfoContainer, 12), (LogContainer, 1)],
    ids=["cluster", "jobs", "logs"],
)
def test_container_element_registration(container_factory, count):
    """
    Verify all containers register their elements without name collisions.
    """
    container = container_factory(adapter=FakeAdapter())
    assert len(container.elements.items) == count
    assert len(container.elements.index) == count


def test_element_store_rejects_duplicates():
    """
    Verify duplicate element names are refused, because they would shadow each other in the output.
    """
    store = ElementStore()
    element = InfoElement(name="foo", label="Foo", sql="SELECT 1;")
    store.add(element)
    with pytest.raises(KeyError) as ex:
        store.add(element)
    assert "Duplicate key/label: foo" in str(ex.value)


def test_info_element_to_dict():
    """
    Verify element serialization, as emitted per the `meta.elements` section.
    """
    element = InfoElement(name="foo", label="Foo", sql="  SELECT 1;  ", description="Foo element", unit="ms")
    data = element.to_dict()
    assert data["name"] == "foo"
    assert data["label"] == "Foo"
    assert data["sql"] == "SELECT 1;"
    assert data["description"] == "Foo element"
    assert data["unit"] == "ms"
    assert data["transform"] == "None"


def test_log_element_limit_templating():
    """
    Verify `LogElement` interpolates its row limit into the SQL statement.
    """
    adapter = FakeAdapter()
    element = LogElement(name="foo", label="Foo", sql="SELECT 1 LIMIT {limit};", limit=42)
    container = LogContainer(adapter=adapter)
    container.evaluate_element(element)
    assert adapter.queries == ["SELECT 1 LIMIT 42;"]


def test_evaluate_element_applies_transform():
    """
    Verify element transformations are applied to the SQL result.
    """
    adapter = FakeAdapter(response=[{"job_count": 42}])
    element = InfoElement(name="foo", label="Foo", sql="SELECT 1;", transform=get_single_value("job_count"))
    container = JobInfoContainer(adapter=adapter)
    assert container.evaluate_element(element) == 42


def test_history_transform_reverses_rows():
    """
    Verify the query history is emitted in chronological order, oldest first.

    The SQL statement selects the most recent jobs first, so the rows are reversed.
    """
    rows = [{"time": 3}, {"time": 2}, {"time": 1}]
    transform = Library.JobInfo.history100.transform
    assert transform is not None
    assert transform(rows) == [{"time": 1}, {"time": 2}, {"time": 3}]


def test_get_single_value():
    """
    Verify scalar reduction of a single-row, single-column SQL result.
    """
    assert get_single_value("job_count")([{"job_count": 42}]) == 42


def test_container_document_shape():
    """
    Verify the `meta`/`data` document shape, and that both sections describe the same elements.
    """
    container = JobInfoContainer(adapter=FakeAdapter())
    document = container.to_dict()

    assert sorted(document) == ["data", "meta"]
    assert sorted(document["meta"]) == ["application_name", "application_version", "elements", "system_time"]
    assert sorted(document["meta"]["elements"]) == JOB_ELEMENT_NAMES
    assert sorted(document["data"]) == JOB_ELEMENT_NAMES


def test_container_meta_includes_sql():
    """
    Verify the emitted metadata echoes the SQL statement of each element, so users can re-run it.
    """
    container = JobInfoContainer(adapter=FakeAdapter())
    elements = container.to_dict()["meta"]["elements"]
    assert elements["running_count"]["sql"].startswith("SELECT")
    assert "sys.jobs" in elements["running_count"]["sql"]


def test_container_by_table_not_implemented():
    """
    Verify the unimplemented per-table inquiry reports itself as such.
    """
    container = JobInfoContainer(adapter=FakeAdapter())
    with pytest.raises(NotImplementedError):
        container.by_table(schema="doc", table="foo")


def test_container_base_needs_builtins():
    """
    Verify containers must register their elements.
    """
    with pytest.raises(NotImplementedError) as ex:
        InfoContainerBase(adapter=FakeAdapter())
    assert ex.match("Method needs to be implemented by child class")
