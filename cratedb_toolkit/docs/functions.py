import dataclasses
import datetime as dt
import logging
from typing import Any, Dict, Optional

import docutils.nodes
from docutils import nodes
from docutils.examples import internals
from docutils.parsers.rst.directives import register_directive
from docutils.parsers.rst.directives.admonitions import Note
from docutils.parsers.rst.roles import normalized_role_options, register_canonical_role  # type: ignore[attr-defined]

from cratedb_toolkit.docs.model import DocsItem
from cratedb_toolkit.docs.util import GenericProcessor

logger = logging.getLogger(__name__)


DOCS_ITEM = DocsItem(
    created=dt.datetime.now().isoformat(),
    generator="CrateDB Toolkit",
    source_url="https://github.com/crate/crate/raw/refs/heads/5.10/docs/general/builtins/scalar-functions.rst",
)


@dataclasses.dataclass
class Function:
    name: str
    signature: str
    category: str
    description: str
    # TODO: Parse `returns` and `example` from `description`.
    returns: Optional[str] = None
    example: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the dataclass instance to a dictionary.

        Returns:
            Dict[str, Any]: A dictionary containing all fields of the instance.
        """
        return dataclasses.asdict(self)


@dataclasses.dataclass
class FunctionRegistry:
    meta: DocsItem = dataclasses.field(default_factory=lambda: DOCS_ITEM)
    functions: Dict[str, Function] = dataclasses.field(default_factory=dict)

    def register(self, function: Function):
        """
        Register a new function in the registry.

        Adds a Function instance to the registry using its signature as the unique key.
        Raises a ValueError if a function with the same signature is already registered.

        Args:
            function: A Function instance to be added to the registry.
        """
        if function.signature in self.functions:
            raise ValueError(f"Function already registered: {function.signature}")
        self.functions[function.signature] = function

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the instance to a dictionary.

        Returns:
            dict: A dictionary containing the instance's fields and their values.
        """
        return dataclasses.asdict(self)


def sphinx_ref_role(role, rawtext, text=None, lineno=None, inliner=None, options=None, content=None):
    options = normalized_role_options(options)
    text = nodes.unescape(text, True)  # type: ignore[attr-defined]
    label = text.split(" ", 1)[0]
    node = nodes.raw(rawtext, label, **options)
    node.source, node.line = inliner.reporter.get_source_and_line(lineno)
    return [node], []


@dataclasses.dataclass
class FunctionsExtractor(GenericProcessor):
    """
    Extract CrateDB functions from documentation.
    Output in JSON, YAML, Markdown, or SQL format.
    """

    registry: FunctionRegistry = dataclasses.field(default_factory=FunctionRegistry)
    thing: Dict[str, Dict[str, Any]] = dataclasses.field(default_factory=dict)
    payload: Optional[str] = None

    def acquire(self):
        """
        Extract and register CrateDB functions from online documentation.

        Fetch documentation from a defined URL, and process its content to extract functions grouped
        under categories. For each function section, it parses the title and description to create a
        Function instance, updates the registry with metadata such as creation time and generator info.
        If no functions are found, the method logs an error and terminates the program. The registry
        is then converted to a dictionary and stored in the instance attribute 'thing'.

        Returns:
            FunctionsExtractor: The instance with an updated function registry.
        """
        register_canonical_role("ref", sphinx_ref_role)
        register_directive("seealso", Note)
        document, pub = internals(DOCS_ITEM.fetch())

        item: docutils.nodes.Element
        function: docutils.nodes.Element
        for item in document:
            if item.tagname == "section":
                category_title = item.children[0].astext()
                for function in item.children:  # type: ignore[assignment]
                    if function.tagname == "section":
                        function_title = function.children[0].astext()
                        function_body = function.children[1].astext()
                        fun = Function(
                            name=function_title.split("(")[0],
                            signature=function_title,
                            category=category_title,
                            description=function_body,
                        )
                        self.registry.register(fun)

        if self.registry.functions:
            self.thing = self.registry.to_dict()
        else:
            logger.error("No functions were extracted. Please check the script or documentation structure.")
        return self
