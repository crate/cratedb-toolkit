# ruff: noqa: T201
import io
import json
import logging
import typing as t

import yaml

if t.TYPE_CHECKING:
    from mcp import ClientSession


logger = logging.getLogger(__name__)


class McpServerCapabilities:
    """
    Wrap database conversations through MCP servers.
    """

    def __init__(self, session: "ClientSession"):
        self.session = session
        self.data: t.Dict[str, t.Any] = {}

    @staticmethod
    def decode_json_text(thing):
        return json.loads(thing.content[0].text)

    def decode_items(self, items):
        import pydantic_core

        return list(map(self.decode_item, json.loads(pydantic_core.to_json(items))))

    @staticmethod
    def decode_item(item):
        try:
            item["text"] = json.loads(item["text"])
        except Exception:  # noqa: S110
            pass
        return item

    async def entity_info(self, fun, attribute):
        from mcp import McpError

        try:
            return self.decode_items(getattr(await fun(), attribute))
        except McpError as e:
            logger.warning(f"Problem invoking method '{fun.__name__}': {e}")

    def add(self, what: str, info: t.List):
        self.data[what] = info

    async def inquire(self):
        # List available prompts
        self.add("prompts", await self.entity_info(self.session.list_prompts, "prompts"))

        # List available resources and resource templates
        self.add("resources", await self.entity_info(self.session.list_resources, "resources"))
        self.add(
            "resource templates", await self.entity_info(self.session.list_resource_templates, "resourceTemplates")
        )

        # List available tools
        self.add("tools", await self.entity_info(self.session.list_tools, "tools"))

    def to_markdown(self):
        buffer = io.StringIO()
        for title, info in self.data.items():
            if not info:
                continue
            buffer.write(f"### {title.title()}\n")
            buffer.write("\n")
            buffer.write("```yaml\n")
            buffer.write(yaml.dump(info, sort_keys=False, width=100))
            buffer.write("```\n")
            buffer.write("\n")
        return buffer.getvalue()

    def to_dict(self):
        return self.data


def to_json(thing):
    return json.dumps(thing, sort_keys=False, indent=2)


def to_yaml(thing):
    return yaml.dump(thing, sort_keys=False)


def format_output(thing, format_: str):
    if format_ == "json":
        return to_json(thing)
    elif format_ == "yaml":
        return to_yaml(thing)
    else:
        raise NotImplementedError(f"Output variant not implemented: {format_}")
