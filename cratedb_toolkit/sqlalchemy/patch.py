from sqlalchemy_cratedb import dialect


def do_executemany(self, cursor, statement, parameters, context=None):
    """
    Improved version of `do_executemany` that stores its response into the request context instance.

    TODO: Refactor this to `sqlalchemy_cratedb.CrateDialect`.
    """
    result = cursor.executemany(statement, parameters)
    if context is not None:
        context.last_executemany_result = result


def monkeypatch_executemany():
    """
    Enable improved version of `do_executemany`.
    """
    dialect.do_executemany = do_executemany
