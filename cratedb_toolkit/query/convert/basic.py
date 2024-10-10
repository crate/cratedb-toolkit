import typing as t

from attr import define


@define
class ObjectAttributeRelocation:
    container_from: str
    container_to: str
    key: str

    def apply(self, data: str) -> str:
        return data.replace(f"{self.container_from}['{self.key}']", f"{self.container_to}['{self.key}']")


def sql_relocate_attribute(data: str, rules: t.List[ObjectAttributeRelocation]) -> str:
    for rule in rules:
        data = rule.apply(data)
    return data


def sql_relocate_pks_dynamodb_ctk_0_0_27(data: str, pks: t.List[str]) -> str:
    rules = [ObjectAttributeRelocation("data", "pk", pk) for pk in pks]
    return sql_relocate_attribute(data, rules)
