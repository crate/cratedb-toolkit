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

    def to_dict(self) -> str:
        """
        Return a deep copy of the formatter's data.
        
        This method returns an independent copy of the data held in the formatter,
        ensuring that modifications to the copy do not affect the original data.
        """
        return deepcopy(self.thing)

    def to_json(self) -> str:
        """
        Converts the stored object to a JSON formatted string.
        
        The JSON string is generated using an indentation of 2 spaces without key sorting, and non-ASCII characters are preserved.
        """
        return json.dumps(self.thing, sort_keys=False, indent=2, ensure_ascii=False)

    def to_yaml(self) -> str:
        """
        Converts the internal data to a YAML formatted string.
        
        Returns:
            str: YAML representation of the data.
        """
        return yaml.dump(self.thing, sort_keys=False)

    def to_markdown(self) -> str:
        """
        Converts the stored data into a Markdown formatted string.
        
        Iterates over each section in the stored data, outputting the section as a second-level header.
        For each key in a section, outputs a third-level header followed by a YAML-formatted code block
        containing the corresponding value. Returns the complete Markdown string.
        """
        buffer = io.StringIO()
        for section, content in self.thing.items():
            buffer.write(f"## {section}")
            for key, value in content.items():
                buffer.write(f"### {key}\n```yaml\n{yaml.dump(value)}```\n\n")
        return buffer.getvalue()

    def to_sql(self):
        """
        Converts the instance data to an SQL query string.
        
        This method serves as a placeholder for SQL formatting. It always raises a
        NotImplementedError, indicating that a custom implementation is required.
            
        Raises:
            NotImplementedError: Always raised since SQL formatting is not implemented.
        """
        raise NotImplementedError("Formatting to SQL needs a custom implementation")
