import dataclasses
import logging
import time
import typing as t
from pathlib import Path

from cratedb_toolkit.cluster.croud import CloudCluster
from cratedb_toolkit.model import InputOutputResource, TableAddress

logger = logging.getLogger(__name__)


class CloudIoSpecs:
    """
    Define capabilities of CrateDB Cloud Import.
    """

    allowed_compressions = ["gzip", None]
    allowed_formats = ["csv", "json", "parquet"]


@dataclasses.dataclass
class CloudJob:
    """
    Manage information about a cloud job.

    It gives quick access to important attributes, without needing to decode
    nested dictionaries.
    """

    info: t.Dict = dataclasses.field(default_factory=dict)
    found: bool = False
    _custom_status: t.Optional[str] = None
    _custom_message: t.Optional[str] = None

    def __post_init__(self):
        self.fix_job_info_table_name()

    @classmethod
    def unknown(cls, message: str):
        cj = cls()
        cj._custom_message = message
        cj._custom_status = "UNKNOWN"
        return cj

    @property
    def id(self):  # noqa: A003
        return self.info["id"]

    @property
    def status(self):
        if self._custom_status:
            return self._custom_status
        return self.info["status"]

    @property
    def success(self):
        return self.status == "SUCCEEDED"

    @property
    def message(self):
        if self._custom_message:
            return self._custom_message
        return self.info["progress"]["message"]

    def fix_job_info_table_name(self):
        """
        Adjust full-qualified table name by adding appropriate quotes.
        Fixes a minor flaw on the upstream API.

        Currently, the API returns `testdrive.pems-1`, but that can not be used at
        all, because it is not properly quoted. It also can not be used 1:1, because
        it is not properly quoted.

        So, converge the table name into `"testdrive"."pems-1"` manually, for a
        full-qualified representation.

        FIXME: Remove after upstream has fixed the flaw.
        """
        job_info = self.info
        if "destination" in job_info and "table" in job_info["destination"]:
            table = job_info["destination"]["table"]
            if '"' not in table and "." in table:
                schema, table = table.split(".")
                table = f'"{schema}"."{table}"'
                job_info["destination"]["table"] = table


class CloudIo:
    """
    Wrap access to CrateDB Cloud Import API.
    """

    def __init__(self, cluster_id: str):
        self.cluster_id = cluster_id
        self.cluster = CloudCluster(cluster_id=cluster_id)

    def load_resource(self, resource: InputOutputResource, target: TableAddress) -> CloudJob:
        """
        Load resource from URL into CrateDB, using CrateDB Cloud infrastructure.

        TODO: Refactor return value to use dedicated type.
        """

        # Use `schema` and `table` when given, otherwise derive from input URL.
        if target.table is None:
            target.table = Path(resource.url).with_suffix("").stem

        logger.info(f"Loading data. source={resource}, target={target}")

        import_job = self.create_import_job(resource=resource, target=target)
        job_id = import_job.id
        # TODO: Review this.
        time.sleep(0.15)
        cloud_job = self.find_job(job_id=job_id)
        if not cloud_job.found:
            logger.error(cloud_job.message)
        if not cloud_job.info:
            cloud_job.info = import_job.info
        return cloud_job

    def find_job(self, job_id: str) -> CloudJob:
        """
        Find CrateDB Cloud job by identifier.
        """
        for job in self.list_jobs():
            if job.id == job_id:
                log_message = f"{job.message} (status: {job.status})"
                if job.success:
                    logger.info(log_message)
                else:
                    logger.error(log_message)
                return job

        return CloudJob.unknown(message=f"Job was not created: {job_id}")

    def list_jobs(self) -> t.Generator[CloudJob, None, None]:
        """
        Inquire API for a list of cloud jobs.
        """
        for job_dict in self.cluster.list_jobs():
            yield CloudJob(info=job_dict, found=True)

    def create_import_job(self, resource: InputOutputResource, target: TableAddress) -> CloudJob:
        """
        Create CrateDB Cloud import job, using resource on filesystem or URL.

        croud clusters import-jobs create from-url --cluster-id e1e38d92-a650-48f1-8a70-8133f2d5c400 \
            --file-format csv --table my_table_name --url https://s3.amazonaws.com/my.import.data.gz --compression gzip
        """

        # Compute command-line arguments for invoking `croud`.
        # FIXME: This call is redundant.
        path = Path(resource.url)

        # Honor `schema` argument.
        if target.schema is not None:
            target.table = f'"{target.schema}"."{target.table}"'

        if resource.compression is None:
            if ".gz" in path.suffixes or ".gzip" in path.suffixes:
                resource.compression = "gzip"

        if resource.format is None:
            if ".csv" in path.suffixes or ".tsv" in path.suffixes:
                resource.format = "csv"
            elif ".json" in path.suffixes or ".jsonl" in path.suffixes or ".ndjson" in path.suffixes:
                resource.format = "json"
            elif ".parquet" in path.suffixes or ".pq" in path.suffixes:
                resource.format = "parquet"

        # Sanity checks.
        if resource.compression not in CloudIoSpecs.allowed_compressions:
            raise NotImplementedError(
                f"Unknown input file format compression: {resource.compression}. "
                f"Use one of: {CloudIoSpecs.allowed_compressions}"
            )
        if resource.format not in CloudIoSpecs.allowed_formats:
            raise NotImplementedError(
                f"Unknown input file format suffixes: {path.suffixes} ({resource.format}). "
                f"Use one of: {CloudIoSpecs.allowed_formats}"
            )

        return CloudJob(info=self.cluster.create_import_job(resource=resource, target=target))
