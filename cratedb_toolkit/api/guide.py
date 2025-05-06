from textwrap import dedent

from cratedb_toolkit.cluster.model import ClusterInformation
from cratedb_toolkit.io.croud import CloudJob
from cratedb_toolkit.util.croud import table_fqn


class GuidingTexts:
    """
    TODO: Add more richness / guidance to the text output.
    """

    def __init__(self, cluster_info: ClusterInformation, job: CloudJob):
        self.cluster_info = cluster_info
        self.job_info = job.info

        self.admin_url = self.cluster_info.cloud["url"]
        self.cluster_name = self.cluster_info.cloud["name"]
        self.table_name = table_fqn(self.job_info["destination"]["table"])

    def success(self):
        return dedent(f"""
        Excellent, that worked well.

        Now, you may want to inquire your data. To do that, use either CrateDB Admin UI,
        or connect on your terminal using `crash`, `ctk shell`, or `psql`.

        The CrateDB Admin UI for your cluster is available at [1]. To easily inspect a
        few samples of your imported data, or to check the cardinality of your database
        table, run [2] or [3]. If you want to export your data again, see [4].

        [1] {self.admin_url}
        [2] ctk shell --cluster-name {self.cluster_name} --command 'SELECT * FROM {self.table_name} LIMIT 10;'
        [3] ctk shell --cluster-name {self.cluster_name} --command 'SELECT COUNT(*) FROM {self.table_name};'
        [4] https://community.cratedb.com/t/cratedb-cloud-news-simple-data-export/1556
        """).strip()  # noqa: S608

    def error(self):
        return dedent("""
        That went south.

        If you can share your import source, we will love to hear from you on our community
        forum [1]. Otherwise, please send us an email [2] about the flaw you've discovered.
        To learn more about the data import feature, see [3].

        [1] https://community.cratedb.com/
        [2] support@crate.io
        [3] https://community.cratedb.com/t/importing-data-to-cratedb-cloud-clusters/1467
        """).strip()
