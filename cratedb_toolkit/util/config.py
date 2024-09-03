import typing as t
from collections import OrderedDict

import attr
from attrs import define
from cattrs.preconf.json import make_converter as make_json_converter
from cattrs.preconf.pyyaml import make_converter as make_yaml_converter


@define
class Metadata:
    version: t.Union[int, None] = None
    type: t.Union[str, None] = None


@define
class Dumpable:
    meta: t.Union[Metadata, None] = None

    def to_dict(self) -> t.Dict[str, t.Any]:
        return attr.asdict(self, dict_factory=OrderedDict, filter=no_privates_no_nulls_no_empties)

    def to_json(self) -> str:
        converter = make_json_converter(dict_factory=OrderedDict)
        return converter.dumps(self.to_dict())

    def to_yaml(self) -> str:
        converter = make_yaml_converter(dict_factory=OrderedDict)
        return converter.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: t.Dict[str, t.Any]):
        return cls(**data)

    @classmethod
    def from_json(cls, json_str: str):
        converter = make_json_converter(dict_factory=OrderedDict)
        return converter.loads(json_str, cls)

    @classmethod
    def from_yaml(cls, yaml_str: str):
        converter = make_yaml_converter(dict_factory=OrderedDict)
        return converter.loads(yaml_str, cls)


def no_privates_no_nulls_no_empties(key, value) -> bool:
    """
    A filter for `attr.asdict`, to suppress private attributes.
    """
    is_private = key.name.startswith("_")
    is_null = value is None
    is_empty = value == []
    if is_private or is_null or is_empty:
        return False
    return True
