import dataclasses
import io
import json
import typing as t
from copy import deepcopy
from enum import Enum

import yaml


class OutputFormat(str, Enum):
    """Output formats supported by the SettingsExtractor."""

    JSON = "json"
    YAML = "yaml"
    MARKDOWN = "markdown"
    SQL = "sql"


@dataclasses.dataclass
class FlexibleFormatter:
    thing: t.Any

    def format(self, format_: t.Union[str, OutputFormat]):
        # Convert the string format to enum if needed.
        if isinstance(format_, str):
            try:
                format_ = OutputFormat(format_.lower())
            except ValueError as e:
                raise ValueError(
                    f"Unsupported format: {format_}. Choose from: {', '.join(f.value for f in OutputFormat)}"
                ) from e

        # Render settings to selected format.
        if format_ == "json":
            return self.to_json()
        elif format_ == "yaml":
            return self.to_yaml()
        elif format_ == "markdown":
            return self.to_markdown()
        elif format_ == "sql":
            return self.to_sql()

        raise NotImplementedError(f"Unsupported format: {format_}")

    def to_dict(self) -> dict:
        return deepcopy(self.thing)

    def to_json(self) -> str:
        return json.dumps(self.thing, sort_keys=False, indent=2, ensure_ascii=False)

    def to_yaml(self) -> str:
        return yaml.dump(self.thing, sort_keys=False)

    def to_markdown(self) -> str:
        buffer = io.StringIO()
        for section, content in self.thing.items():
            buffer.write(f"## {section}")
            for key, value in content.items():
                buffer.write(f"### {key}\n```yaml\n{yaml.dump(value)}```\n\n")
        return buffer.getvalue()

    def to_sql(self):
        raise NotImplementedError("Formatting to SQL needs a custom implementation")
