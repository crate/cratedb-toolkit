import dataclasses
import typing as t
from pathlib import Path

from cratedb_toolkit.util.format import FlexibleFormatter, OutputFormat


@dataclasses.dataclass
class GenericProcessor:
    """
    Extract CrateDB settings from documentation.
    Output in JSON, YAML, Markdown, or SQL format.
    """

    thing: t.Any
    formatter: t.Type[FlexibleFormatter] = dataclasses.field(default=FlexibleFormatter)
    payload: t.Optional[str] = None

    def render(self, format_: t.Union[str, OutputFormat]):
        # Render settings to selected format.
        formatter = self.formatter(self.thing)
        self.payload = formatter.format(format_)

        return self

    def write(self, path: t.Optional[Path] = None):
        if self.payload is None:
            raise ValueError("No content to write. Please invoke `render()` first.")
        if path is None:
            print(self.payload)  # noqa: T201
        else:
            with open(path, "w", encoding="utf-8") as f:
                f.write(self.payload)
        return self
