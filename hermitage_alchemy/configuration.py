import collections.abc
import dataclasses
import typing

import sqlalchemy

TOTAL_QUERY_FIELD = "_total"


@dataclasses.dataclass
class TrackUnit:
    name: str
    qua: str | None = None
    label: str | None = None

    @property
    def qualified_name(self):
        parts = [self.name, self.qua or '', self.label or '']
        return ":".join(parts)

    def __str__(self):
        return self.label or self.qua or self.name


class Space:
    def __init__(self, name: str):
        self._track = []
        for part in name.split('__'):
            if ':' in part:
                a = part.split(':')
                if len(a) > 2:
                    name, qua, label = a
                else:
                    name, qua = a
                    label = None
            else:
                name, qua, label = part, None, None
            self._track.append(TrackUnit(name, qua if qua else None, label if label else None))

    @property
    def qualified_name(self) -> str:
        return "__".join(map(lambda u: u.qualified_name, self._track))

    @property
    def last(self) -> str:
        return self._track[-1].name

    def slice(self, start: int | None, end: int | None) -> typing.Self | None:
        _slice = self._track[start:end]
        if _slice:
            return Space('__'.join(list(map(str, _slice))))

    def __len__(self):
        return len(self._track)

    def __iter__(self):
        yield from self._track

    def __add__(self, other: str | typing.Self):
        if isinstance(other, str):
            return Space(f'{self.qualified_name}__{other}')
        else:
            return Space(f'{self.qualified_name}__{other.qualified_name}')

    def __getitem__(self, item: int):
        return self._track[item]

    def __bool__(self):
        return bool(self._track)

    def __str__(self):
        return '__'.join(list(map(str, self._track)))


class Address:
    def __init__(self, name: str, space: Space):
        self._name = name
        self._space = space

    @property
    def qualified_name(self) -> str:
        return f'{self._space.qualified_name}__{self._name}'

    @property
    def name(self):
        return self._name

    @property
    def space(self):
        return self._space

    def __str__(self):
        return f'{str(self._space)}__{self._name}'


@dataclasses.dataclass
class Link:
    source_address: Address
    target_address: Address


@dataclasses.dataclass
class M2O(Link):
    pass


@dataclasses.dataclass
class O2M(Link):
    pass


@dataclasses.dataclass
class M2M(Link):
    interim_source_address: Address
    interim_target_address: Address


@dataclasses.dataclass(frozen=True)
class Fk:
    target_address: Address
    parent_address: Address

    def __eq__(self, other: TrackUnit) -> bool:
        return other.name == self.target_address.space.last and (
            other.qua is None or f'{other.qua}_id' == self.parent_address.name
        )


class Schema:
    def __init__(
        self,
        tables: collections.abc.Iterable[sqlalchemy.Table],
        *foreign_keys: tuple[sqlalchemy.Column, sqlalchemy.Column]
    ):
        self._graph = collections.defaultdict(set)
        self._tables = {}
        self._cache = {}
        self._index_tables(tables)
        self._index_custom_foreign_keys(*foreign_keys)

    def get_link(
        self,
        source_space: Space,
        target_space: Space
    ):
        link = self._get_m2o(source_space, target_space)
        if not link:
            link = self._get_o2m(source_space, target_space)
        if not link:
            link = self._get_m2m(source_space, target_space)
        return link

    def get_table(self, space: Space) -> sqlalchemy.Table | None:
        if str(space) in self._cache:
            return self._cache[str(space)]
        last_unit = space[-1]
        result = self._tables.get(last_unit.name)
        if last_unit.label or last_unit.qua:
            result = result.alias(str(space))
            self._cache[str(space)] = result
        return result

    def get_column(self, address: Address) -> sqlalchemy.Column | None:
        try:
            address_string = str(address)
            if address_string not in self._cache:
                column = getattr(
                    self.get_table(address.space).c, address.name
                )
                space = address.space.slice(1, None)
                if space:
                    column = column.label(str(Address(address.name, space)))
                self._cache[address_string] = column
            return self._cache[address_string]
        except AttributeError:
            return None

    def _index_tables(
        self,
        tables: collections.abc.Iterable[sqlalchemy.Table]
    ):
        for table in tables:
            self._tables[table.name] = table
            for fk in table.foreign_keys:
                self._graph[fk.parent.table.name].add(
                    Fk(
                        target_address=Address(name=fk.column.name, space=Space(fk.column.table.name)),
                        parent_address=Address(name=fk.parent.name, space=Space(fk.parent.table.name))
                    )
                )

    def _index_custom_foreign_keys(self, *foreign_keys: tuple[sqlalchemy.Column, sqlalchemy.Column]):
        for parent, target in foreign_keys:
            self._graph[parent.table.name].add(
                Fk(
                    parent_address=Address(name=parent.name, space=Space(parent.table.name)),
                    target_address=Address(name=target.name, space=Space(target.table.name))
                )
            )

    def _get_m2o(
        self,
        source_space: Space,
        target_space: Space
    ) -> M2O | None:
        for t, fks in self._graph.items():
            if source_space.last == t:
                for fk in fks:
                    if target_space[-1] == fk:
                        return M2O(
                            source_address=fk.parent_address,
                            target_address=fk.target_address
                        )

    def _get_o2m(
        self,
        source_space: Space,
        target_space: Space,
    ) -> O2M | None:
        for t, fks in self._graph.items():
            if target_space.last == t:
                for fk in fks:
                    if source_space[-1] == fk:
                        return O2M(
                            source_address=fk.target_address,
                            target_address=fk.parent_address
                        )

    def _get_m2m(
        self,
        source_space: Space,
        target_space: Space,
    ) -> M2M | None:
        for t, fks in self._graph.items():
            source_fk = None
            target_fk = None
            for fk in fks:
                if source_space[-1] == fk:
                    source_fk = fk
                if target_space[-1] == fk:
                    target_fk = fk
                if source_fk and target_fk:
                    return M2M(
                        source_address=Address(source_fk.target_address.name, source_space),
                        interim_source_address=Address(source_fk.parent_address.name, Space(t)),
                        target_address=Address(target_fk.target_address.name, target_space),
                        interim_target_address=Address(target_fk.parent_address.name, Space(t)),
                    )
