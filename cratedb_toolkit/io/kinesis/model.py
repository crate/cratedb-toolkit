import typing as t

from attr import Factory
from attrs import define
from commons_codec.model import ColumnType, ColumnTypeMapStore, PrimaryKeyStore, TableAddress
from tikray.model.base import SchemaDefinition
from tikray.model.collection import CollectionAddress
from tikray.model.project import ProjectTransformation

from cratedb_toolkit.util.config import Dumpable


@define
class SettingsDefinition:
    ignore_ddl: bool = True


@define
class ColumnTypeDefinition(SchemaDefinition):
    pass


@define
class PrimaryKeyDefinition(SchemaDefinition):
    @property
    def names(self):
        return [rule.pointer.lstrip("/") for rule in self.rules]


@define
class CollectionDefinition(Dumpable):
    address: t.Union[CollectionAddress, None] = None
    settings: t.Union[SettingsDefinition, None] = None
    pk: t.Union[PrimaryKeyDefinition, None] = None
    map: t.Union[ColumnTypeDefinition, None] = None


@define
class RecipeDefinition(ProjectTransformation):
    collections: t.List[CollectionDefinition] = Factory(list)
    _map: t.Dict[CollectionAddress, CollectionDefinition] = Factory(dict)

    def codec_options(self) -> t.Tuple[PrimaryKeyStore, ColumnTypeMapStore]:
        pks = PrimaryKeyStore()
        cms = ColumnTypeMapStore()
        for collection in self.collections:
            if not collection.address:
                continue
            ta = TableAddress(schema=collection.address.container, table=collection.address.name)
            if collection.pk:
                pks[ta] = collection.pk.names
            if collection.map:
                for item in collection.map.rules:
                    cms.add(ta, item.pointer.lstrip("/"), ColumnType(item.type))
        return pks, cms
