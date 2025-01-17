# ruff: noqa: B018, ERA001, S608, T201
# Source:
# https://docs.marimo.io/guides/working_with_data/dataframes.html
# https://github.com/marimo-team/marimo/blob/main/examples/sql/querying_dataframes.py

import marimo

__generated_with = "0.10.13"
app = marimo.App(width="full", app_title="CrateDB Jobs Analysis")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        """
        # Querying `stats.jobstats_statements` with Marimo

        The SQL statement has a LIMIT set, which is adjustable by the _Slider_.
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    limitsl = mo.ui.slider(1, 30000, step=20, value=100)
    limitsl
    return (limitsl,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## `stats.jobstats_statements`""")
    return


@app.cell(hide_code=True)
def _(limitsl):
    import os

    import pandas as pd
    import sqlalchemy as sa

    cratedb_sqlalchemy_url = os.getenv("CRATEDB_SQLALCHEMY_URL", "crate://?schema=stats")
    engine = sa.create_engine(cratedb_sqlalchemy_url)
    df = pd.read_sql(
        sql=f"""
        SELECT stmt, username, query_type, last_used, avg_duration, bucket
        FROM stats.jobstats_statements
        ORDER BY last_used, avg_duration DESC
        LIMIT {limitsl.value}""",
        con=engine,
    )
    # df2 = pd.read_sql(f"SELECT * FROM sys.jobs_log ORDER BY ended LIMIT {limitjl.value}", con=engine)

    # Convert epoch milliseconds to datetime
    df["last_used"] = pd.to_datetime(df["last_used"], unit="ms")

    # Convert Bucket to separate COLUMNS in the
    bucket_df = pd.DataFrame(df["bucket"].tolist())
    result_df = pd.concat([df.drop("bucket", axis=1), bucket_df], axis=1)

    # Keep the BUCKET times in a specific Order
    cols = bucket_df.columns.tolist()
    numeric_cols = [col for col in cols if col != "INF"]
    sorted_numeric_cols = sorted(numeric_cols, key=lambda x: int(x))
    sorted_cols = sorted_numeric_cols + ["INF"]

    # Reorder the DataFrame columns
    bucket_df = bucket_df[sorted_cols]
    result_df = pd.concat([df.drop("bucket", axis=1), bucket_df[sorted_cols]], axis=1)

    # mo.ui.table(
    #    result_df,
    #    pagination=True,
    #    page_size=23  # Shows 50 rows per page
    # )

    # Get the oldest statement
    first_row = result_df.loc[result_df["last_used"].idxmin(), ["stmt", "last_used"]]
    print(f"Oldest recorded statement ({first_row['last_used']}):\n{first_row['stmt']}\n")

    # Get the most recent statement
    last_row = result_df.loc[result_df["last_used"].idxmax(), ["stmt", "last_used"]]
    print(f"Last recorded statement ({last_row['last_used']}):\n{last_row['stmt']}")

    result_df
    return (
        bucket_df,
        cols,
        df,
        engine,
        first_row,
        last_row,
        numeric_cols,
        pd,
        result_df,
        sa,
        sorted_cols,
        sorted_numeric_cols,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Bucket Summary""")
    return


@app.cell(hide_code=True)
def _(result_df, sorted_cols):
    bucket_summary = result_df[sorted_cols].sum().reset_index(name="query_count").rename(columns={"index": "bucket"})
    bucket_summary
    return (bucket_summary,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Query Performance per `Query_Type`""")
    return


@app.cell
def _(result_df):
    query_type_performance = (
        result_df.groupby("query_type").agg({"avg_duration": "mean"}).sort_values(by="avg_duration", ascending=False)
    )
    query_type_performance
    return (query_type_performance,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Slow Queries by User""")
    return


@app.cell(hide_code=True)
def _(result_df):
    slow_queries_by_user = (
        result_df.groupby("username")
        .agg({"avg_duration": "sum"})
        .sort_values(by="avg_duration", ascending=False)
        .head(10)
    )
    slow_queries_by_user
    return (slow_queries_by_user,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""##  Compute Weighted Execution Time Stats""")
    return


@app.cell(hide_code=True)
def _(bucket_df, df, pd):
    import matplotlib.pyplot as plt
    import numpy as np

    # Extract bucket columns (excluding 'INF' if present)
    bucket_cols = [col for col in bucket_df.columns if col != "INF"]
    numeric_buckets = sorted(map(int, bucket_cols))  # Convert to sorted integers

    # Append INF bucket as a high value (e.g., 10x the max bucket)
    inf_value = max(numeric_buckets) * 10
    numeric_buckets.append(inf_value)

    # Add 'INF' to dataframe for processing
    bucket_df["INF"] = bucket_df.get("INF", 0)  # Handle missing INF column

    # Compute weighted average and standard deviation
    def weighted_stats(row):
        counts = row[bucket_cols + ["INF"]].fillna(0).to_numpy()  # Execution counts per bucket
        durations = np.array(numeric_buckets)  # Corresponding execution time thresholds

        if counts.sum() == 0:
            return pd.Series({"weighted_avg": np.nan, "weighted_std": np.nan})

        weighted_avg = np.average(durations, weights=counts)
        weighted_std = np.sqrt(np.average((durations - weighted_avg) ** 2, weights=counts))

        return pd.Series({"weighted_avg": weighted_avg, "weighted_std": weighted_std})

    # Apply function to each row
    variability_df = bucket_df.apply(weighted_stats, axis=1)

    # Add query statements for reference
    variability_df["stmt"] = df["stmt"]

    # Sort by highest execution time variability
    variability_df = variability_df.sort_values(by="weighted_std", ascending=False)

    # Show top 10 queries with highest execution time variability
    # print(variability_df.head(10))
    variability_df
    return (
        bucket_cols,
        inf_value,
        np,
        numeric_buckets,
        plt,
        variability_df,
        weighted_stats,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""---""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# sys.jobs_log""")
    return


@app.cell
def _(mo):
    limitjl = mo.ui.slider(1, 30000, step=20, value=100)
    limitjl
    return (limitjl,)


@app.cell(hide_code=True)
def _(engine, limitjl, pd):
    import ast  # Safely parse string representations of dictionaries

    # df2 = pd.read_sql(sql="SELECT stmt, started, ended, username FROM sys.jobs_log limit 10", con=engine)
    # df2 = pd.read_sql(sql="SELECT * FROM sys.jobs_log limit {$limit.value} order by ended", con=engine)
    df2 = pd.read_sql(f"SELECT * FROM sys.jobs_log ORDER BY ended LIMIT {limitjl.value}", con=engine)

    # Convert epoch milliseconds to datetime
    df2["started"] = pd.to_datetime(df2["started"], unit="ms")
    df2["ended"] = pd.to_datetime(df2["ended"], unit="ms")
    df2["duration"] = (df2["ended"] - df2["started"]).dt.total_seconds() * 1000

    # Example: Assuming the column is named 'metadata'
    df2["node"] = df2["node"].apply(lambda x: ast.literal_eval(x)["name"] if isinstance(x, str) else x["name"])

    # mo.ui.dataframe(df2)
    # Aggregating the 'duration' column
    # aggregated = df2['duration'].agg(['min', 'max', 'mean']).reset_index()

    # Rename the columns for clarity
    # aggregated.columns = ['aggregation', 'duration_min', 'duration_max', 'duration_mean']

    # Add aggregated values as new columns to df2
    # df2 = df2.join(aggregated[['duration_min', 'duration_max', 'duration_mean']].iloc[0], rsuffix='_agg')

    df2_next = df2
    df2_next = df2_next[["stmt", "started", "ended", "duration", "username", "node"]]
    df2_next
    return ast, df2, df2_next


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Slow Query Log""")
    return


@app.cell
def _(df2_next, mo):
    max_value = len(df2_next)
    slowq = mo.ui.slider(1, max_value, step=20, value=10)
    slowq
    return max_value, slowq


@app.cell
def _(df2_next, slowq):
    # Sort by 'duration' in descending order and get the top 10 slowest queries
    # slowest_queries = df2_next.sort_values(by='duration', ascending=False).head(10)
    # Sort by 'duration' in descending order and get the top 10 slowest queries
    slowq_value = int(slowq.value)  # Convert the slider value to an integer

    # Sort the DataFrame by 'duration' and get the top slowq_value rows
    slowest_queries = df2_next.sort_values(by="duration", ascending=False).head(slowq_value)
    # slowest_queries = df2_next.sort_values(by='duration', ascending=False).head(slowq.value)

    # Display the result
    print(slowest_queries[["stmt", "duration"]])

    # Display the result
    slowest_queries
    return slowest_queries, slowq_value


@app.cell
def _(df):
    df
    return


if __name__ == "__main__":
    app.run()
