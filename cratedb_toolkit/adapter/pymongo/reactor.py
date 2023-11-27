import typing as t
from collections import abc

import sqlalchemy as sa
from jessiql import Query, QueryObject, QueryObjectDict
from jessiql.exc import InvalidColumnError
from jessiql.typing import SARowDict


def table_to_model(table: sa.Table) -> t.Type[sa.orm.Mapper]:
    """
    Create SQLAlchemy model class from Table object.

    - https://docs.sqlalchemy.org/en/14/orm/mapping_styles.html#imperative-mapping
    - https://sparrigan.github.io/sql/sqla/2016/01/03/dynamic-tables.html
    """
    mapper_registry = sa.orm.registry(metadata=table.metadata)
    Surrogate = type("Surrogate", (), {})
    mapper_registry.map_imperatively(Surrogate, table)
    return Surrogate


def reflect_model(engine: t.Any, metadata: sa.MetaData, table_name: str) -> t.Type[sa.orm.Mapper]:
    """
    Create SQLAlchemy model class by reflecting a database table.
    """
    table = sa.Table(table_name, metadata, autoload_with=engine)
    return table_to_model(table)


def mongodb_query(
    model: t.Type[sa.orm.Mapper],
    select: t.Union[t.List, None] = None,
    filter: t.Union[t.Dict[str, t.Any], None] = None,  # noqa: A002
    sort: t.Union[t.List[str], None] = None,
) -> Query:
    """
    Create a JessiQL Query object from an SQLAlchemy model class and typical MongoDB query parameters.
    """

    select = select or list(model._sa_class_manager.keys())  # type: ignore[attr-defined]

    filter = filter or {}  # noqa: A001
    sort = sort or []

    # TODO: select, filter, sort, skip, limit
    if "_id" in filter:
        filter["_id"] = str(filter["_id"])
    query_dict = QueryObjectDict({"select": select, "filter": filter, "sort": sort})
    query_object = QueryObject.from_query_object(query_dict)

    try:
        return Query(query=query_object, Model=model)
    except InvalidColumnError as ex:
        msg = str(ex)
        if "Invalid column" in msg and "specified in filter" in msg:
            return EmptyQuery()
        else:
            raise


class EmptyQuery(Query):
    """
    A surrogate QueryExecutor for propagating back empty results.
    """

    def __init__(self, *args, **kwargs):
        self.related_executors = {}

    def _load_results(self, *args, **kwargs) -> abc.Iterator[SARowDict]:
        raise StopIteration()

    def _apply_operations_to_results(self, *args, **kwargs) -> t.List[SARowDict]:
        return []
