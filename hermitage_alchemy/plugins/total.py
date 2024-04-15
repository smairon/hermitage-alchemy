import collections
import typing

import sqlalchemy

from ..definition import contracts

FIELD_NAME = "total"


class Beacon(contracts.MetaElement):
    pass


def query_builder_plugin(
    element: Beacon,
    columns: collections.abc.MutableSequence[sqlalchemy.Column] | None,
    *args
):
    if isinstance(element, Beacon):
        if columns is None:
            columns = []
        columns.append(sqlalchemy.text(f"count(*) over () as {FIELD_NAME}"))

    return columns, *args


def remove_beacon(data: collections.abc.MutableMapping):
    if FIELD_NAME in data:
        del data[FIELD_NAME]
    return data


class TotalPlugin(contracts.ReadPlugin):
    @classmethod
    def get_beacon(cls) -> type[Beacon]:
        return Beacon

    @classmethod
    def get_query_builder_plugin(cls) -> collections.abc.Callable:
        return query_builder_plugin

    def get_mappers(self) -> collections.abc.Iterable[collections.abc.Callable]:
        return [remove_beacon]

    def get_result(self) -> dict[str, typing.Any] | None:
        if self._data:
            return {FIELD_NAME: self._data[0][FIELD_NAME]}
        else:
            return {FIELD_NAME: 0}
