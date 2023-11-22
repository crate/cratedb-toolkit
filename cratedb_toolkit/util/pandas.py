import typing as t

from pandas.io.sql import SQLTable


def patch_pandas_io_sqldatabase_with_dialect_parameters(table_kwargs: t.Dict = None):
    """
    When using pandas' `to_sql` function, configure the SQLAlchemy database dialect
    implementation using custom dialect parameters.

    https://github.com/pandas-dev/pandas/issues/45259
    """

    # Set defaults.
    table_kwargs = table_kwargs or {}

    # Provide enhancement code.
    class SQLTableWithDialectParameters(SQLTable):
        def _create_table_setup(self):
            table = super()._create_table_setup()
            table.kwargs.update(table_kwargs)
            return table

    # Activate enhancement code.
    import pandas.io.sql

    pandas.io.sql.SQLTable = SQLTableWithDialectParameters
