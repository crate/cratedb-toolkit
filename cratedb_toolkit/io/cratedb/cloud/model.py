import dataclasses
import typing as t

from cratedb_toolkit.util.croud import table_fqn


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
        return self.info.get("id")

    @property
    def status(self):
        if self._custom_status:
            return self._custom_status
        return self.info.get("status", "UNKNOWN")

    @property
    def success(self):
        return self.status == "SUCCEEDED"

    @property
    def message(self):
        if self._custom_message:
            return self._custom_message
        return self.info.get("progress", {}).get("message", "No message available")

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
               https://github.com/crate/croud/issues/566
        """
        job_info = self.info
        if "destination" in job_info and "table" in job_info["destination"]:
            job_info["destination"]["table"] = table_fqn(job_info["destination"]["table"])


class CloudIoSpecs:
    """
    Define capabilities of CrateDB Cloud Import.
    """

    allowed_compressions: t.ClassVar[tuple] = ("gzip", None)
    allowed_formats: t.ClassVar[tuple] = ("csv", "json", "parquet")
