# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import sqlalchemy as sa

from tests.conftest import TESTDRIVE_DATA_SCHEMA


def test_cratedb_summits(cratedb):
    """
    Just to verify communication with CrateDB works.
    """
    database_url = cratedb.get_connection_url()

    sa_engine = sa.create_engine(database_url)
    with sa_engine.connect() as conn:
        sql = "SELECT mountain, region, prominence FROM sys.summits ORDER BY prominence DESC LIMIT 3;"
        with conn.execute(sa.text(sql)) as result:
            assert result.fetchall() == [
                ("Mont Blanc", "Mont Blanc massif", 4695),
                ("Großglockner", "Glockner Group", 2428),
                ("Finsteraarhorn", "Bernese Alps", 2280),
            ]


def test_database_insert(cratedb):
    """
    Verify that inserting two records and reading them back works well.
    """
    cratedb.reset()

    database_url = cratedb.get_connection_url()

    # Validate data in storage system.
    sa_engine = sa.create_engine(database_url)
    with sa_engine.connect() as conn:
        # Define and materialize table schema.
        meta = sa.MetaData()
        tbl = sa.Table(
            "testdrive",
            meta,
            sa.Column("username", sa.String),
            sa.Column("fullname", sa.String),
            schema=TESTDRIVE_DATA_SCHEMA,
        )
        meta.create_all(sa_engine)

        # Insert data.
        insertable = sa.insert(tbl)
        conn.execute(insertable.values(username="foo", fullname="Full Foo"))
        conn.execute(insertable.values(username="bar", fullname="Full Bar"))

        # Synchronize data.
        conn.exec_driver_sql(f'REFRESH TABLE "{TESTDRIVE_DATA_SCHEMA}"."testdrive";')

        # Verify data.
        sql = f'SELECT COUNT(*) FROM "{TESTDRIVE_DATA_SCHEMA}"."testdrive";'  # noqa: S608
        with conn.execute(sa.text(sql)) as result:
            assert result.scalar_one() == 2
