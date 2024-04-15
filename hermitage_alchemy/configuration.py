import collections.abc
import typing

import sqlalchemy

from .definition import contracts


class Schema:
    def __init__(
        self,
        tables: collections.abc.Iterable[sqlalchemy.Table],
        fk_postfix: str = '_id'
    ):
        self._instances = {}
        self._tables = {}
        self._links = {}
        self._fk_postfix = fk_postfix
        self._fk = collections.defaultdict(dict)
        self._index_tables(tables)

    def register_link(
        self,
        namespace: contracts.Namespace,
        link: contracts.Link
    ):
        self._links[namespace] = link

    def get_table(self, namespace: contracts.Namespace) -> contracts.Table | None:
        if isinstance(namespace, contracts.Namespace):
            return self._resolve_namespace(namespace)[-1]

    def get_column(self, element: contracts.InvoiceElement) -> sqlalchemy.Column | None:
        table = self.get_table(element.namespace)
        if table is not None:
            return table(element.address)

    def get_fk_table_alias(
        self,
        address: contracts.Address
    ) -> str | None:
        return str(address).replace(self._fk_postfix, '')

    def get_m2o(
        self,
        namespace: contracts.Namespace
    ) -> contracts.M2O | None:
        if link := self._links.get(namespace):
            if isinstance(link, contracts.M2O):
                return link
        else:
            chain = self._resolve_namespace(namespace)
            if len(chain) < 2:
                return
            source_table = chain[-2]
            target_table = chain[-1]
            if source_table and target_table:
                if (f_keys := self._fk.get(str(source_table))) and str(namespace.last()) in f_keys:
                    self._links[namespace] = contracts.M2O(
                        source_table=source_table,
                        target_table=target_table,
                        source_column=contracts.Address(f_keys[str(namespace.last())][0].name),
                        target_column=contracts.Address(f_keys[str(namespace.last())][1].name),
                    )
            return self._links.get(namespace)

    def get_m2m(
        self,
        namespace: contracts.Namespace
    ) -> contracts.M2M | None:
        if link := self._links.get(namespace):
            if isinstance(link, contracts.M2M):
                return link
        else:
            chain = self._resolve_namespace(namespace, True) or ()
            if len(chain) < 2:
                return
            source_table = chain[-2]
            target_table = chain[-1]
            if namespace not in self._links:
                for interim_table_name, _map in self._fk.items():
                    if len(_map) < 2:
                        continue
                    source_column = None
                    target_column = None
                    interim_source_column = None
                    interim_target_column = None
                    for _cols in _map.values():
                        if _cols[1].table.name == str(source_table):
                            source_column = contracts.Address(_cols[1].name)
                            interim_source_column = contracts.Address(_cols[0].name)
                        elif _cols[1].table.name == str(target_table):
                            target_column = contracts.Address(_cols[1].name)
                            interim_target_column = contracts.Address(_cols[0].name)

                    if source_column and target_column:
                        self._links[namespace] = contracts.M2M(
                            source_table=source_table,
                            target_table=target_table,
                            source_column=source_column,
                            target_column=target_column,
                            interim_table=self.get_table(contracts.Namespace(interim_table_name)),
                            interim_source_column=interim_source_column,
                            interim_target_column=interim_target_column
                        )
                        return self._links[namespace]

    def get_o2m(
        self,
        namespace: contracts.Namespace,
    ) -> contracts.O2M | None:
        if link := self._links.get(namespace):
            if isinstance(link, contracts.O2M):
                return link
        else:
            chain = self._resolve_namespace(namespace, True) or ()
            if len(chain) < 2:
                return
            source_table = chain[-2]
            target_table = chain[-1]
            if namespace not in self._links:
                fk_keys = self._fk.get(str(target_table))
                if fk_keys:
                    t = str(namespace[-2])
                    if t not in fk_keys:
                        t = next((k for k, v in fk_keys.items() if v[1].table.name == t), None)
                    if t:
                        self._links[namespace] = contracts.O2M(
                            source_table=source_table,
                            target_table=target_table,
                            source_column=contracts.Address(fk_keys[t][1].name),
                            target_column=contracts.Address(fk_keys[t][0].name),
                        )
            return self._links.get(namespace)

    def _index_tables(
        self,
        tables: collections.abc.Iterable[sqlalchemy.Table]
    ) -> typing.NoReturn:
        for table in tables:
            self._instances[table.name] = table
            for fk in table.foreign_keys:
                self._fk[table.name][fk.parent.name.replace(self._fk_postfix, '')] = (fk.parent, fk.column)

    def _resolve_namespace(
        self,
        namespace: contracts.Namespace,
        strict: bool = False
    ) -> list[contracts.Table] | None:
        parts = []
        previous = None
        for _name in namespace:
            _name = str(_name)
            if _name in self._instances:
                parts.append(_name)
            if strict:
                continue
            if fk := self._fk.get(previous, {}).get(_name):
                previous = fk[1].table.name
                parts.append(previous)
            elif fk := self._fk.get(_name, {}).get(previous):
                previous = fk[1].table.name
                parts.insert(-1, previous)
            previous = _name

        if len(parts) == len(namespace):
            result = []
            for i, table_name in enumerate(parts, 1):
                table_alias = str(namespace.part(i))
                if table_alias not in self._tables:
                    self._tables[table_alias] = contracts.Table(
                        self._instances[table_name],
                        table_alias if table_alias != self._instances[table_name].name else None
                    )
                result.append(self._tables[table_alias])
            return result
