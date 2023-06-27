"""
Implements a retention policy by reallocating cold partitions

A detailed tutorial is available at https://community.crate.io/t/cratedb-and-apache-airflow-building-a-hot-cold-storage-data-retention-policy/934

Prerequisites
-------------
- CrateDB 5.2.0 or later
- Tables for storing retention policies need to be created once manually in
  CrateDB. See the file setup/data_retention_schema.sql in this repository.
"""
from pathlib import Path
import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task


@task
def get_policies(ds=None):
    """Retrieve all partitions effected by a policy"""
    pg_hook = PostgresHook(postgres_conn_id="cratedb_connection")
    sql = Path("include/data_retention_retrieve_reallocate_policies.sql")
    return pg_hook.get_records(
        sql=sql.read_text(encoding="utf-8"),
        parameters={"day": ds},
    )


def map_policy(policy):
    """Map index-based policy to readable dict structure"""
    return {
        "schema": policy[0],
        "table": policy[1],
        "table_fqn": policy[2],
        "column": policy[3],
        "value": policy[4],
        "attribute_name": policy[5],
        "attribute_value": policy[6],
    }


@dag(
    start_date=pendulum.datetime(2021, 11, 19, tz="UTC"),
    schedule="@daily",
    catchup=False,
    template_searchpath=["include"],
)
def data_retention_reallocate():
    SQLExecuteQueryOperator.partial(
        task_id="reallocate_partitions",
        conn_id="cratedb_connection",
        sql="""
            ALTER TABLE {{params.table_fqn}} PARTITION ({{params.column}} = {{params.value}})
            SET ("routing.allocation.require.{{params.attribute_name}}" = '{{params.attribute_value}}');
            """,
    ).expand(params=get_policies().map(map_policy))


data_retention_reallocate()
