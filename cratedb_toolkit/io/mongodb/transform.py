import logging
import typing as t
from pathlib import Path

from jsonpointer import JsonPointer
from zyp.model.collection import CollectionAddress, CollectionTransformation
from zyp.model.project import TransformationProject

logger = logging.getLogger(__name__)


class TransformationManager:
    def __init__(self, path: Path):
        self.path = path
        self.active = False
        if not self.path:
            return
        if not self.path.exists():
            raise FileNotFoundError(f"File does not exist: {self.path}")
        self.project = TransformationProject.from_yaml(self.path.read_text())
        logger.info("Transformation manager initialized. File: %s", self.path)
        self.active = True

    def apply_type_overrides(self, database_name: str, collection_name: str, collection_schema: t.Dict[str, t.Any]):
        if not self.active:
            return
        address = CollectionAddress(database_name, collection_name)
        try:
            transformation: CollectionTransformation = self.project.get(address)
        except KeyError:
            return
        logger.info(f"Applying type overrides for {database_name}/{collection_name}")
        # TODO: Also support addressing nested elements.
        #       Hint: Implementation already exists on another machine,
        #       where it has not been added to the repository. Sigh.
        for rule in transformation.schema.rules:
            pointer = JsonPointer(f"/document{rule.pointer}/types")
            type_stats = pointer.resolve(collection_schema)
            type_stats[rule.type] = {"count": int(9e10)}

    def apply_transformations(self, database_name: str, collection_name: str, data: t.Dict[str, t.Any]):
        if not self.active:
            return data
        address = CollectionAddress(database_name, collection_name)
        try:
            transformation: CollectionTransformation = self.project.get(address)
        except KeyError:
            return data
        if transformation.bucket:
            return transformation.bucket.apply(data)
        return data
