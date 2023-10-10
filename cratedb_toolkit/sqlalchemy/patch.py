import typing as t

import sqlalchemy as sa


def patch_inspector():
    """
    When using `get_table_names()`, make sure the correct schema name gets used.

    Apparently, SQLAlchemy does not honor the `search_path` of the engine, when
    using the inspector?

    FIXME: Bug in CrateDB SQLAlchemy dialect?
    """

    def get_effective_schema(engine: sa.Engine):
        schema_name_raw = engine.url.query.get("schema")
        schema_name = None
        if isinstance(schema_name_raw, str):
            schema_name = schema_name_raw
        elif isinstance(schema_name_raw, tuple):
            schema_name = schema_name_raw[0]
        return schema_name

    from crate.client.sqlalchemy.dialect import CrateDialect

    get_table_names_dist = CrateDialect.get_table_names

    def get_table_names(self, connection: sa.Connection, schema: t.Optional[str] = None, **kw: t.Any) -> t.List[str]:
        if schema is None:
            schema = get_effective_schema(connection.engine)
        return get_table_names_dist(self, connection=connection, schema=schema, **kw)

    CrateDialect.get_table_names = get_table_names  # type: ignore
