import dataclasses
import datetime as dt
import logging
import sys
from typing import Any, Dict, Optional

import docutils.nodes
import requests
from docutils import nodes
from docutils.examples import internals
from docutils.parsers.rst.directives import register_directive
from docutils.parsers.rst.directives.admonitions import Note
from docutils.parsers.rst.roles import normalized_role_options, register_canonical_role  # type: ignore[attr-defined]

from cratedb_toolkit.docs.util import GenericProcessor

logger = logging.getLogger(__name__)


DOCS_URL = "https://github.com/crate/crate/raw/refs/heads/5.10/docs/general/builtins/scalar-functions.rst"


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
        return dataclasses.asdict(self)


@dataclasses.dataclass
class FunctionRegistry:
    meta: Dict[str, str] = dataclasses.field(default_factory=dict)
    functions: Dict[str, Function] = dataclasses.field(default_factory=dict)

    def register(self, function: Function):
        if function.signature in self.functions:
            raise ValueError(f"Function already registered: {function.signature}")
        self.functions[function.signature] = function

    def to_dict(self) -> Dict[str, Any]:
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
        register_canonical_role("ref", sphinx_ref_role)
        register_directive("seealso", Note)
        document, pub = internals(requests.get(DOCS_URL, timeout=10).text)

        self.registry.meta["created"] = dt.datetime.now().isoformat()
        self.registry.meta["generator"] = "CrateDB Toolkit"

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

        if not self.registry.functions:
            logger.error("No functions were extracted. Please check the script or documentation structure.")
            sys.exit(1)
        self.thing = self.registry.to_dict()
        return self
