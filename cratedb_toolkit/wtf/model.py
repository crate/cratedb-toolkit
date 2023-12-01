# Copyright (c) 2021-2024, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import dataclasses
import typing as t
from abc import abstractmethod

from cratedb_toolkit.util import DatabaseAdapter
from cratedb_toolkit.wtf.util import get_baseinfo


@dataclasses.dataclass
class InfoElement:
    name: str
    label: str
    sql: str
    description: t.Union[str, None] = None
    transform: t.Union[t.Callable, None] = None
    unit: t.Union[str, None] = None

    def to_dict(self):
        data = dataclasses.asdict(self)
        data["sql"] = data["sql"].strip()
        data["transform"] = str(data["transform"])
        return data


@dataclasses.dataclass
class LogElement(InfoElement):
    limit: int = 100


@dataclasses.dataclass
class ElementStore:
    items: t.List[InfoElement] = dataclasses.field(default_factory=list)
    index: t.Dict[str, InfoElement] = dataclasses.field(default_factory=dict)

    def add(self, *elements: InfoElement):
        for element in elements:
            self.items.append(element)
            if element.name in self.index:
                raise KeyError(f"Duplicate key/label: {element.name}")
            self.index[element.name] = element


class InfoContainerBase:
    def __init__(self, adapter: DatabaseAdapter, scrub: bool = False):
        self.adapter = adapter
        self.scrub = scrub
        self.elements = ElementStore()
        self.register_builtins()

    @abstractmethod
    def register_builtins(self):
        raise NotImplementedError("Method needs to be implemented by child class")

    def metadata(self):
        data = {}
        data.update(get_baseinfo())
        data["elements"] = {}
        for element in self.elements.items:
            data["elements"][element.name] = element.to_dict()
        return data

    def evaluate_element(self, element: InfoElement):
        sql = element.sql
        if isinstance(element, LogElement):
            sql = sql.format(limit=element.limit)
        results = self.adapter.run_sql(sql, records=True)
        if element.transform is not None:
            results = element.transform(results)
        return results

    def to_dict(self, data=None):
        if data is None:
            data = self.render()
        return {"meta": self.metadata(), "data": data}

    def render(self):
        data = {}
        for element in self.elements.items:
            data[element.name] = self.evaluate_element(element)
        return data

    # FIXME
    def by_table(self, schema: str, table: str):
        raise NotImplementedError("Please implement InfoContainerBase.by_table")
