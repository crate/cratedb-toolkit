import threading
import time

from cratedb_toolkit.testing.testcontainers.cratedb import CrateDBTestAdapter


class BackgroundProcessor:
    """
    Manage event processor / stream consumer in separate thread, consuming forever.
    """

    delay_start = 0.25
    delay_step = 0.25

    def __init__(self, loader, cratedb: CrateDBTestAdapter):
        self.loader = loader
        self.cratedb = cratedb
        self.table_name = self.loader.cratedb_table
        self.thread = threading.Thread(target=self.loader.start)

    def __enter__(self):
        """
        Start stream consumer.
        """
        self.thread.start()
        time.sleep(self.delay_start)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Stop stream consumer.
        """
        self.loader.stop()
        self.thread.join()

    def __next__(self):
        time.sleep(self.delay_step)
        self.cratedb.database.refresh_table(self.table_name)
