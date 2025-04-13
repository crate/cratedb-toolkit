import dataclasses
import io
import json
import typing as t
from copy import deepcopy
from enum import Enum

import yaml


class OutputFormat(str, Enum):
    """Possible supported output formats."""

    JSON = "json"
    YAML = "yaml"
    MARKDOWN = "markdown"
    SQL = "sql"


@dataclasses.dataclass
class FlexibleFormatter:
    thing: t.Any

    def format(self, format_: t.Union[str, OutputFormat]):
        """
        Formats the data to the specified output format.

        If a string is provided for the format, it is converted to a supported
        OutputFormat in a case-insensitive manner. Supported formats are 'json',
        'yaml', 'markdown', and 'sql'. The method delegates formatting to the
        corresponding conversion method. A ValueError is raised if the string
        does not match any supported format, and a NotImplementedError is raised
        if the specified format is not implemented.

        Args:
            format_ (Union[str, OutputFormat]): The desired output format.

        Returns:
            str: The formatted data.

        Raises:
            ValueError: If the provided format string is unrecognized.
            NotImplementedError: If the specified format is unsupported.
        """
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
        if not isinstance(self.thing, dict):
            raise NotImplementedError(f"Unable to convert to Markdown: {self.thing}")
        buffer = io.StringIO()
        for section, content in self.thing.items():
            buffer.write(f"## {section}\n\n")
            if isinstance(content, dict):
                for key, value in content.items():
                    buffer.write(f"### {key}\n```yaml\n{yaml.dump(value, sort_keys=False)}```\n\n")
            else:
                buffer.write(f"{str(content)}\n\n")
        return buffer.getvalue()

    def to_sql(self) -> str:
        raise NotImplementedError("Formatting to SQL needs a domain-specific implementation")
