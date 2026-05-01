import functools


def function(f):
    """Execute a function on a compute cluster."""

    # Wrap payload function into being submitted to a dask worker.
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        address = None
        try:
            import dask

            address = dask.config.get("scheduler-address", None)
        except ImportError:
            pass
        if address is None:
            # No remote scheduler configured; invoke directly without Dask overhead.
            return f(*args, **kwargs)

        from dask.distributed import Client

        with Client(address) as client:
            return client.submit(f, *args, **kwargs).result()

    return wrapper
