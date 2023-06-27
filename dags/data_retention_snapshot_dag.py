"""
Implements a retention policy by snapshotting expired partitions to a repository

A detailed tutorial is available at https://community.crate.io/t/cratedb-and-apache-airflow-building-a-data-retention-policy-using-external-snapshot-repositories/1001

Prerequisites
-------------
In CrateDB, tables for storing retention policies need to be created once manually.
See the file setup/data_retention_schema.sql in this repository.
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
    sql = Path("include/data_retention_retrieve_snapshot_policies.sql")
    return pg_hook.get_records(
        sql=sql.read_text(encoding="utf-8"), parameters={"day": ds}
    )


def map_policy(policy):
    """Map index-based policy to readable dict structure"""
    return {
        "schema": policy[0],
        "table": policy[1],
        "table_fqn": policy[2],
        "column": policy[3],
        "value": policy[4],
        "target_repository_name": policy[5],
    }


@dag(
    start_date=pendulum.datetime(2021, 11, 19, tz="UTC"),
    schedule="@daily",
    catchup=False,
)
def data_retention_snapshot():
    policies = get_policies().map(map_policy)

    reallocate = SQLExecuteQueryOperator.partial(
        task_id="snapshot_partitions",
        conn_id="cratedb_connection",
        sql="""
            CREATE SNAPSHOT {{params.target_repository_name}}."{{params.schema}}.{{params.table}}-{{params.value}}"
            TABLE {{params.table_fqn}} PARTITION ({{params.column}} = {{params.value}})
            WITH ("wait_for_completion" = true);
            """,
    ).expand(params=policies)

    delete = SQLExecuteQueryOperator.partial(
        task_id="delete_partitions",
        conn_id="cratedb_connection",
        sql="DELETE FROM {{params.table_fqn}} WHERE {{params.column}} = {{params.value}};",
    ).expand(params=policies)

    reallocate >> delete


data_retention_snapshot()
