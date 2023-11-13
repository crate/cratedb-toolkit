import logging
import time
import typing as t
from pathlib import Path

from cratedb_toolkit.job.croud import jobs_list
from cratedb_toolkit.model import InputOutputResource, TableAddress
from cratedb_toolkit.util.croud import CroudCall, CroudWrapper

logger = logging.getLogger(__name__)


class CloudIoSpecs:
    """
    Define capabilities of CrateDB Cloud Import.
    """

    allowed_compressions = ["gzip", None]
    allowed_formats = ["csv", "json", "parquet"]


class CloudIo:
    """
    Wrap access to CrateDB Cloud Import API.
    """

    def __init__(self, cluster_id: str):
        self.cluster_id = cluster_id

    def load_resource(self, resource: InputOutputResource, target: TableAddress) -> t.Tuple[t.Dict, bool]:
        """
        Load resource from URL into CrateDB, using CrateDB Cloud infrastructure.
        """

        # Use `schema` and `table` when given, otherwise derive from input URL.
        if target.table is None:
            target.table = Path(resource.url).with_suffix("").stem

        logger.info(f"Loading data. source={resource}, target={target}")

        import_job = self.create_import_job(resource=resource, target=target)
        job_id = import_job["id"]
        time.sleep(0.15)
        outcome, success, found = self.find_job(job_id=job_id)
        if not found:
            logger.error(f"Job not found: {job_id}")
        if not outcome:
            outcome = import_job
        return outcome, success

    def find_job(self, job_id: str) -> t.Tuple[t.Dict, bool, bool]:
        """
        Find CrateDB Cloud job by identifier.
        """
        jobs = jobs_list(self.cluster_id)

        found = False
        success = False
        job_info: t.Dict = {}
        for job in jobs:
            if job["id"] == job_id:
                found = True
                job_info = job
                status = job["status"]
                message = job["progress"]["message"]
                message = f"{message} (status: {status})"
                if status == "SUCCEEDED":
                    success = True
                    logger.info(message)
                else:
                    logger.error(message)
                break

        fix_job_info_table_name(job_info)

        return job_info, success, found

    def create_import_job(self, resource: InputOutputResource, target: TableAddress) -> t.Dict[str, t.Any]:
        """
        Create CrateDB Cloud import job, using resource on filesystem or URL.

        croud clusters import-jobs create from-url --cluster-id e1e38d92-a650-48f1-8a70-8133f2d5c400 \
            --file-format csv --table my_table_name --url https://s3.amazonaws.com/my.import.data.gz --compression gzip
        """
        from croud.__main__ import import_job_create_common_args
        from croud.clusters.commands import import_jobs_create_from_file, import_jobs_create_from_url
        from croud.parser import Argument

        specs: t.List[Argument] = import_job_create_common_args
        arguments = []

        url_argument = Argument("--url", type=str, required=True, help="The URL the import file will be read from.")

        file_id_argument = Argument(
            "--file-id",
            type=str,
            required=False,
            help="The file ID that will be used for the "
            "import. If not specified then --file-path"
            " must be specified. "
            "Please refer to `croud organizations "
            "files` for more info.",
        )
        file_path_argument = Argument(
            "--file-path",
            type=str,
            required=False,
            help="The file in your local filesystem that "
            "will be used. If not specified then "
            "--file-id must be specified. "
            "Please note the file will become visible "
            "under `croud organizations files list`.",
        )

        # Compute command-line arguments for invoking `croud`.
        path = Path(resource.url)

        # Honor `schema` argument.
        table = target.table
        if target.schema is not None:
            table = f'"{target.schema}"."{target.table}"'

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

        # TODO: Sanitize table name. Which characters are allowed?
        if path.exists():
            specs.append(file_path_argument)
            specs.append(file_id_argument)
            arguments = [
                f"--cluster-id={self.cluster_id}",
                f"--file-path={resource.url}",
                f"--table={table}",
                f"--file-format={resource.format}",
            ]
            fun = import_jobs_create_from_file
        else:
            specs.append(url_argument)
            arguments = [
                f"--cluster-id={self.cluster_id}",
                f"--url={resource.url}",
                f"--table={table}",
                f"--file-format={resource.format}",
            ]
            fun = import_jobs_create_from_url

        if resource.compression is not None:
            arguments += [f"--compression={resource.compression}"]

        call = CroudCall(
            fun=fun,
            specs=specs,
            arguments=arguments,
        )

        wr = CroudWrapper(call=call)
        job_info = wr.invoke()

        # Adjust full-qualified table name by adding appropriate quotes.
        # FIXME: Remove after flaw has been fixed.
        fix_job_info_table_name(job_info)

        return job_info


def fix_job_info_table_name(job_info: t.Dict[str, t.Any]):
    # FIXME: Remove after upstream has fixed the flaw.
    #        Currently, the API returns `testdrive.pems-1`, but that can not be used at all.
    #        So, converge it into `"testdrive"."pems-1"`.
    if "destination" in job_info and "table" in job_info["destination"]:
        table = job_info["destination"]["table"]
        if '"' not in table and "." in table:
            schema, table = table.split(".")
            table = f'"{schema}"."{table}"'
            job_info["destination"]["table"] = table
