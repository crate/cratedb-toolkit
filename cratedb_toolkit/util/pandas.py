import typing as t

import sqlalchemy as sa


def patch_pandas_sqltable_with_dialect_parameters(table_kwargs: t.Dict = None):
    """
    When using pandas' `to_sql` function, configure the SQLAlchemy database dialect
    implementation using custom dialect parameters.

    https://github.com/pandas-dev/pandas/issues/45259
    """

    # Set defaults.
    table_kwargs = table_kwargs or {}

    # Provide enhancement code.
    # It needs to be inlined in order to obtain `table_kwargs`.
    from pandas.io.sql import SQLTable

    class SQLTableWithDialectParameters(SQLTable):
        def _create_table_setup(self):
            table = super()._create_table_setup()
            table.kwargs.update(table_kwargs)
            return table

    # Activate enhancement code.
    import pandas.io.sql

    pandas.io.sql.SQLTable = SQLTableWithDialectParameters


def patch_pandas_sqltable_with_extended_mapping():
    """
    Improve pandas' `to_sql` function to respect CrateDB's advanced data types.
    """

    # It needs to be inlined in order to be composable with `SQLTableWithDialectParameters`.
    from pandas.io.sql import SQLTable

    class SQLTableWithDtypeMappersForCrateDB(SQLTable):
        def _get_column_names_and_types(self, dtype_mapper, errors: str = "raise"):
            column_names_and_types = super()._get_column_names_and_types(dtype_mapper)
            column_names_and_types_new = []
            for name, sqlalchemy_type, is_index in column_names_and_types:
                # TODO: Currently only handles columns. Also make it handle indexes.
                if is_index:
                    if errors == "raise":
                        raise ValueError("pandas indexes not supported yet")

                else:
                    values = self.frame._get_value(0, name)
                    if isinstance(values, list):
                        first_value = values[0]
                        first_value_type = type(first_value)
                        # TODO: Use `boltons.remap`.
                        sqlalchemy_type = ARRAY_TYPE_MAP.get(first_value_type)
                        if sqlalchemy_type is None:
                            raise TypeError(f"Data type not supported yet: List[{first_value_type}]")

                column_names_and_types_new.append((name, sqlalchemy_type, is_index))

            return column_names_and_types_new

        def _execute_create(self) -> None:
            # Prevent `InvalidColumnNameException["_id" conflicts with system column pattern]`.
            # FIXME: Review, and solve differently.
            ddl = self.sql_schema()
            ddl = ddl.replace('"_id" STRING,', "")
            with self.pd_sql.run_transaction():
                self.pd_sql.con.exec_driver_sql(ddl)

    # Activate enhancement code.
    import pandas.io.sql

    pandas.io.sql.SQLTable = SQLTableWithDtypeMappersForCrateDB


ARRAY_TYPE_MAP = {
    str: sa.ARRAY(sa.TEXT),
    int: sa.ARRAY(sa.INTEGER),
    float: sa.ARRAY(sa.FLOAT),
}
