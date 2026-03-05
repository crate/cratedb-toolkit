import logging
import time
import typing as t
from pathlib import Path

from cratedb_toolkit.cluster.croud import CloudClusterServices
from cratedb_toolkit.io.cratedb.cloud.model import CloudIoSpecs, CloudJob
from cratedb_toolkit.model import InputOutputResource, TableAddress

logger = logging.getLogger(__name__)


class CloudIo:
    """
    Wrap access to CrateDB Cloud Import API.
    """

    def __init__(self, cluster_id: str):
        self.cluster_id = cluster_id
        self.cluster = CloudClusterServices(cluster_id=cluster_id)

    def load_resource(
        self, resource: InputOutputResource, target: TableAddress, max_retries: int = 20, retry_delay: float = 0.15
    ) -> CloudJob:
        """
        Load resource from URL into CrateDB, using CrateDB Cloud infrastructure.
        """

        # Use `schema` and `table` when given, otherwise derive from input URL.
        if target.table is None:
            target.table = Path(resource.url).with_suffix("").stem

        logger.info(f"Loading data. source={resource}, target={target}")

        import_job = self.create_import_job(resource=resource, target=target)
        job_id = import_job.id

        # Find the submitted job per CrateDB Cloud API.
        for _ in range(max_retries):
            job = self.find_job(job_id=job_id)
            if job.found:
                break
            time.sleep(retry_delay)
        else:
            msg = "Job never appeared in the listing"
            logger.error(msg)
            raise RuntimeError(msg)

        if not job.found:
            logger.error(f"Job not found: {job.message}")
        if not job.info:
            job.info = import_job.info
        return job

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
