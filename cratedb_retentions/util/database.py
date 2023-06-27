import sqlalchemy as sa


def run_sql(dburi: str, sql: str):
    """
    Connect to database, invoke SQL statement, and return results.
    Run SQL statement
    """
    sa_engine = sa.create_engine(dburi, echo=True)
    with sa_engine.connect() as conn:
        result = conn.execute(sa.text(sql))
        return result.fetchall()
