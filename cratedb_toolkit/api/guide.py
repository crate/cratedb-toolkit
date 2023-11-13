class GuidingTexts:
    """
    TODO: Add more richness / guidance to the text output.
    """

    def __init__(self, admin_url: str = None, table_name: str = None):
        self.admin_url = admin_url
        self.table_name = table_name

    def success(self):
        return f"""
        Excellent, that worked well.

        Now, you may want to inquire your data. To do that, use either CrateDB Admin UI,
        or connect on your terminal using `crash`, `ctk shell`, or `psql`.

        The CrateDB Admin UI for your cluster is available at [1]. To easily inspect a
        few samples of your imported data, or to check the cardinality of your database
        table, run [2] or [3]. If you want to export your data again, see [4].

        [1] {self.admin_url}
        [2] ctk shell --command 'SELECT * FROM {self.table_name} LIMIT 10;'
        [3] ctk shell --command 'SELECT COUNT(*) FROM {self.table_name};'
        [4] https://community.cratedb.com/t/cratedb-cloud-news-simple-data-export/1556
        """  # noqa: S608

    def error(self):
        return """
        That went south.

        If you can share your import source, we will love to hear from you on our community
        forum [1]. Otherwise, please send us an email [2] about the flaw you've discovered.
        To learn more about the data import feature, see [3].

        [1] https://community.cratedb.com/
        [2] support@crate.io
        [3] https://community.cratedb.com/t/importing-data-to-cratedb-cloud-clusters/1467
        """
