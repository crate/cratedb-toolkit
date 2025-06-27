import typing as t
from pathlib import Path

from boltons.urlutils import URL

from cratedb_toolkit.io.kinesis.model import RecipeDefinition
from cratedb_toolkit.io.kinesis.relay import KinesisRelay
from cratedb_toolkit.util.data import asbool

TransformationType = t.Union[Path, RecipeDefinition, None]


def kinesis_relay(source_url: URL, target_url: str, recipe: TransformationType = None):
    once = asbool(source_url.query_params.get("once", "false"))

    if recipe is None:
        parsed_recipe = None
    elif isinstance(recipe, RecipeDefinition):
        parsed_recipe = recipe
    elif isinstance(recipe, Path):
        parsed_recipe = RecipeDefinition.from_yaml(recipe.read_text())
    else:
        raise TypeError(f"Unsupported recipe type: {type(recipe)}")

    ka = KinesisRelay(str(source_url), target_url, recipe=parsed_recipe)
    ka.start(once=once)
