"""

ctk dms table-mappings -a abc,def -s public

ctk dms table-mappings -a "postgresql://username:password@postgresql.example.com:5432/postgres" -s public
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import attr
import sqlalchemy as sa


@attr.define
class TableMappingBuilder:
    """
    # Generate mapping rules for DMS replication task.
    # https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.Kinesis.html
    # https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.CustomizingTasks.TableMapping.html
    """

    schema: str
    names: List[str]
    rules: List[Dict[str, Any]] = attr.field(factory=list)

    def build(self) -> "TableMappingBuilder":
        self.add_selection()
        for name in self.names:
            self.add_mapping(name)
        return self

    def render(self) -> str:
        payload = {"rules": self.rules}
        return json.dumps(payload, indent=4)

    def add_rule(
        self,
        name: str,
        type: str,  # noqa: A002
        action: str,
        locator: Dict[str, str],
        id: Union[str, None] = None,  # noqa: A002
        filters: List[Any] = None,
        mapping_parameters: Dict[str, str] = None,
    ) -> "TableMappingBuilder":
        if id is None:
            id = str(len(self.rules) + 1)  # noqa: A001
        rule: Dict[str, Any] = {
            "rule-id": id,
            "rule-name": name,
            "rule-type": type,
            "rule-action": action,
            "object-locator": locator,
        }
        if filters:
            rule["filters"] = filters
        if mapping_parameters:
            rule["mapping-parameters"] = mapping_parameters
        self.rules.append(rule)
        return self

    def add_selection(self) -> "TableMappingBuilder":
        """
        The table selector rule allows wildcards ("%").
        """
        self.add_rule(
            name=f"include-{self.schema}",
            type="selection",
            action="include",
            locator={
                "schema-name": self.schema,
                "table-name": "%",
            },
        )
        return self

    def add_mapping(self, name: str) -> "TableMappingBuilder":
        """
        The object mapping rule needed for converging to Kinesis does not allow wildcards.

        Using the percent wildcard ("%") in "table-settings" rules is
        not supported for source databases as shown following:

            Error: Exact schema and table required when using object mapping rule with '3.5' engine.

        https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.CustomizingTasks.TableMapping.SelectionTransformation.Tablesettings.html#CHAP_Tasks.CustomizingTasks.TableMapping.SelectionTransformation.Tablesettings.Wildcards
        """
        self.add_rule(
            name=f"map-{name}",
            type="object-mapping",
            action="map-record-to-record",
            locator={
                "schema-name": self.schema,
                "table-name": name,
            },
            mapping_parameters={
                "partition-key-type": "schema-table",
            },
        )
        return self


def get_table_names(address: str, schema: Optional[str] = None) -> List[str]:
    """
    Extract table names from PostgreSQL, file, or comma-separated list.
    """
    if address.startswith("postgresql://"):
        engine = sa.create_engine(address)
        names = sa.inspect(engine).get_table_names(schema=schema)
    elif Path(address).is_file():
        # TODO: Extract table names from a) plaintext file or b) SQL file including DDL statements.
        raise NotImplementedError("Reading table names from file not implemented yet")
    else:
        names = list(map(str.strip, address.split(",")))
    return names
