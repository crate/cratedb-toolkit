import dataclasses
import typing as t
from pathlib import Path

from cratedb_toolkit.util.format import FlexibleFormatter, OutputFormat


@dataclasses.dataclass
class GenericProcessor:
    """
    Extract CrateDB knowledge bites (e.g., settings, functions) from documentation.
    Output in JSON, YAML, Markdown, or SQL format.
    """

    thing: t.Any
    formatter: t.Type[FlexibleFormatter] = dataclasses.field(default=FlexibleFormatter)
    payload: t.Optional[str] = None

    def render(self, format_: t.Union[str, OutputFormat]):
        """
        Render the processor's content into the selected output format.

        This method uses the configured formatter to convert the stored data into a formatted
        string according to the provided format. The formatted output is saved to the payload
        attribute, and the instance is returned.

        Args:
            format_ (Union[str, OutputFormat]): The desired output format.

        Returns:
            GenericProcessor: The current instance with the updated payload.
        """
        formatter = self.formatter(self.thing)
        self.payload = formatter.format(format_)

        return self

    def write(self, path: t.Optional[Path] = None):
        """
        Write the formatted payload to a file or print it to the console.

        If a file path is provided, the payload is written to the specified file using UTF-8
        encoding. If no path is provided, the payload is printed to standard output. Raises
        a ValueError if no payload is available, indicating that the render() method must be
        called first.

        Parameters:
            path (Optional[Path]): The file path to which the payload should be written. If None,
                                   the payload is printed.

        Returns:
            GenericProcessor: The instance of the processor.
        """
        if self.payload is None:
            raise ValueError("No content to write. Please invoke `render()` first.")
        if path is None:
            print(self.payload)  # noqa: T201
        else:
            with open(path, "w", encoding="utf-8") as f:
                f.write(self.payload)
        return self
