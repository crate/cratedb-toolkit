import datetime as dt
import logging
import sys
import time
import typing as t

import attr
import colorlog
import orjson
import sqlparse
import yaml

from cratedb_toolkit.model import TableAddress
from cratedb_toolkit.util import DatabaseAdapter

logger = logging.getLogger(__name__)


@attr.define
class SysJobsLog:
    """
    Represent a single record in CrateDB's `sys.jobs_log` table.
    """

    id: str
    started: int
    ended: int
    classification: t.Dict[str, t.Any]
    stmt: str
    error: str
    node: t.Dict[str, t.Any]
    username: str

    @property
    def template(self) -> str:
        return "{timestamp} [{duration}] {label:17s}: {message} SQL: {sql:50s}"

    @property
    def label(self):
        red = colorlog.escape_codes.escape_codes["red"]
        green = colorlog.escape_codes.escape_codes["green"]
        reset = colorlog.escape_codes.escape_codes["reset"]
        if self.error:
            return f"{red}ERROR{reset}"
        else:
            return f"{green}INFO{reset}"

    @property
    def duration(self) -> int:
        return self.ended - self.started

    @property
    def started_iso(self) -> str:
        return str(dt.datetime.fromtimestamp(self.started / 1000))[:-3]

    @property
    def duration_iso(self) -> str:
        d = dt.timedelta(seconds=self.duration)
        return str(d)

    @property
    def classification_str(self) -> str:
        type_ = self.classification.get("type")
        labels = ",".join(self.classification.get("labels", []))
        return f"{type_}: {labels}"

    def to_log(self, format: str):  # noqa: A002
        sql = self.stmt
        if "pretty" in format:
            sql = "\n" + sqlparse.format(sql, reindent=True, keyword_case="upper")
        item = {
            "timestamp": self.started_iso,
            "duration": self.duration_iso,
            "label": self.label,
            "sql": sql,
            "message": self.error or "Success",
        }
        return self.template.format(**item)


@attr.define
class TableTailer:
    """
    Tail a table, optionally following its tail for new records.
    """

    db: DatabaseAdapter
    resource: TableAddress
    interval: t.Optional[float] = None
    format: t.Optional[str] = None

    def __attrs_post_init__(self):
        self.db.internal = True
        if self.interval is None:
            self.interval = 0.5
        if not self.format:
            if self.resource.fullname == "sys.jobs_log":
                self.format = "log"
            else:
                self.format = "json"

    def start(self, lines: int = 10, follow: bool = False):
        name = self.resource.fullname
        constraint = "1 = 1"
        if self.resource.fullname == "sys.jobs_log":
            constraint = f"stmt NOT LIKE '%{self.db.internal_tag}'"
        total = self.db.count_records(name, where=constraint)
        offset = total - lines
        if offset < 0:
            offset = 0
        while True:
            sql = f"SELECT * FROM {name} WHERE {constraint} OFFSET {offset}"  # noqa: S608
            result = self.db.run_sql(sql, records=True)
            for item in result:
                if self.format and self.format.startswith("log"):
                    if self.resource.fullname == "sys.jobs_log":
                        record = SysJobsLog(**item)
                        sys.stdout.write(record.to_log(format=self.format))
                        sys.stdout.write("\n")
                    else:
                        raise NotImplementedError(
                            "Log output only implemented for `sys.jobs_log`, use `--format={json,yaml}"
                        )
                elif self.format == "json":
                    sys.stdout.write(orjson.dumps(item).decode("utf-8"))
                    sys.stdout.write("\n")
                elif self.format == "yaml":
                    sys.stdout.write("---\n")
                    sys.stdout.write(yaml.dump(item))
                    sys.stdout.write("\n")
                else:
                    raise NotImplementedError(f"Output format not implemented: {self.format}")
            if not follow:
                return result
            offset += len(result)
            time.sleep(t.cast(float, self.interval))
