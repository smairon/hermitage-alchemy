import typing
import functools
import dataclasses

import sqlalchemy.ext.asyncio
import collections.abc

from hermitage.definition import contracts as hermitage_contracts

AsyncEngineContract = sqlalchemy.ext.asyncio.AsyncEngine
AsyncConnectionContract = sqlalchemy.ext.asyncio.AsyncConnection
StorageBucketCollectionContract = collections.abc.Iterable[sqlalchemy.Table]

Namespace = hermitage_contracts.Namespace
Address = hermitage_contracts.Address
Invoice = hermitage_contracts.Invoice
InvoiceElement = hermitage_contracts.InvoiceElement
Clause = hermitage_contracts.Clause
Item = hermitage_contracts.Item
Row = hermitage_contracts.Row
LogicalOperator = hermitage_contracts.LogicalOperator
View = hermitage_contracts.View
MetaElement = hermitage_contracts.MetaElement
Plugin = hermitage_contracts.Plugin
ReadPlugin = hermitage_contracts.ReadPlugin
WritePlugin = hermitage_contracts.WritePlugin
PluginRegistry = hermitage_contracts.PluginRegistry


class ReadAsyncConnectionContract(AsyncConnectionContract):
    pass


class WriteAsyncConnectionContract(AsyncConnectionContract):
    pass


class ShutdownContext(typing.TypedDict):
    exc_type: type[Exception]
    exc_val: Exception


class Table:
    def __init__(
        self,
        table: sqlalchemy.Table,
        alias: str | None = None
    ):
        self._table = table
        self.alias = alias.replace('.', '__') if alias else None

    def __call__(
        self,
        column_address: Address | None = None,
        column_alias: str | None = None
    ) -> sqlalchemy.Table | sqlalchemy.Column:
        if column_address is None:
            return self._instance

        column = getattr(self._instance.c, str(column_address))
        if column_alias:
            column = column.label(column_alias)

        return column

    @functools.cached_property
    def _instance(self):
        return self._table.alias(self.alias) if self.alias else self._table

    @functools.cached_property
    def _name(self):
        return self._table.name

    def __str__(self):
        return self._table.name


@dataclasses.dataclass
class Link:
    source_table: Table
    target_table: Table
    source_column: Address
    target_column: Address

    def __str__(self):
        return f'{str(self.source_table)}__{str(self.target_table)}'


@dataclasses.dataclass
class M2O(Link):
    pass


@dataclasses.dataclass
class O2M(Link):
    pass


@dataclasses.dataclass
class M2M(Link):
    interim_table: Table
    interim_source_column: Address
    interim_target_column: Address


class ReadClientContract(typing.Protocol):
    async def __call__(self, invoice: Invoice) -> View: ...


class WriteClientContract(typing.Protocol):
    async def __call__(self, invoice: Invoice) -> View | None: ...
