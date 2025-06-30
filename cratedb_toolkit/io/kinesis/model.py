import typing as t

import attrs
from attr import Factory
from attrs import define
from commons_codec.model import ColumnType, ColumnTypeMapStore, PrimaryKeyStore, TableAddress
from tikray.model.base import SchemaDefinition
from tikray.model.collection import CollectionAddress
from tikray.model.project import ProjectTransformation

from cratedb_toolkit.util.config import Dumpable


@define
class SettingsDefinition:
    mapping_strategy: str = "direct"
    ignore_ddl: bool = False


@define
class ColumnTypeDefinition(SchemaDefinition):
    pass


@define
class PrimaryKeyDefinition(SchemaDefinition):
    @property
    def names(self) -> t.List[str]:
        return [rule.pointer.lstrip("/") for rule in self.rules]


@define
class CollectionDefinition(Dumpable):
    address: t.Union[CollectionAddress, None] = None
    settings: t.Union[SettingsDefinition, None] = attrs.Factory(SettingsDefinition)
    pk: t.Union[PrimaryKeyDefinition, None] = None
    map: t.Union[ColumnTypeDefinition, None] = None


@define
class RecipeDefinition(ProjectTransformation):
    collections: t.List[CollectionDefinition] = Factory(list)

    def codec_options(self) -> t.Tuple[PrimaryKeyStore, ColumnTypeMapStore, t.Dict, t.Dict]:
        pks = PrimaryKeyStore()
        cms = ColumnTypeMapStore()
        mapping_strategy = {}
        ignore_ddl = {}
        for collection in self.collections:
            if not collection.address:
                continue
            ta = TableAddress(schema=collection.address.container, table=collection.address.name)
            if collection.pk:
                pks[ta] = collection.pk.names
            if collection.map:
                for item in collection.map.rules:
                    cms.add(ta, item.pointer.lstrip("/"), ColumnType(item.type))
            if collection.settings:
                mapping_strategy[ta] = collection.settings.mapping_strategy
                ignore_ddl[ta] = collection.settings.ignore_ddl
        return pks, cms, mapping_strategy, ignore_ddl
