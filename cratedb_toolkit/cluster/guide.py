import shlex
from textwrap import dedent

from cratedb_toolkit.cluster.model import ClusterInformation
from cratedb_toolkit.io.croud import CloudJob
from cratedb_toolkit.util.croud import table_fqn


class DataImportGuide:
    """
    Guiding texts for CLI interface usability.

    Args:
        cluster_info: ClusterInformation object containing cluster details
        job: CloudJob object containing job information

    TODO: Add more richness / guidance to the text output.
    """

    def __init__(self, cluster_info: ClusterInformation, job: CloudJob):
        self.cluster_info = cluster_info
        self.job_info = job.info

        self.admin_url = self.cluster_info.cloud.get("url") or "<unknown-url>"
        self.cluster_name = shlex.quote(self.cluster_info.cloud.get("name") or "<unknown-cluster>")

        table_name = self.job_info.get("destination", {}).get("table")
        self.table_name = table_fqn(table_name) if table_name else None

    def success(self):
        return dedent(f"""
        Excellent, that worked well.

        Now, you may want to inquire your data. To do that, use either CrateDB Admin UI,
        or connect on your terminal using `crash`, `ctk shell`, or `psql`.

        The CrateDB Admin UI for your cluster is available at [1].
        {
            ("To inspect imported data, run [2] or [3].")
            if self.table_name
            else "Your import is pending or failed; guidelines only available when a destination table exists."
        }
        If you want to export your data again, see [4].

        [1] {self.admin_url}
        {
            f"[2] ctk shell --cluster-name {self.cluster_name} --command 'SELECT * FROM {self.table_name} LIMIT 10;'"
            if self.table_name
            else ""
        }
        {
            f"[3] ctk shell --cluster-name {self.cluster_name} --command 'SELECT COUNT(*) FROM {self.table_name};'"
            if self.table_name
            else ""
        }
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
