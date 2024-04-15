import collections.abc

import sqlalchemy

from hermitage.definition import contracts
from ..configuration import Schema
from ..assembling import FilterCompiler
from ..definition import contracts

SaverDataContract = contracts.Row | contracts.Clause


def invoice_separator(
    invoice: contracts.Invoice
) -> tuple[list[contracts.Row] | None, contracts.Clause | None, list[contracts.MetaElement] | None]:
    data = None
    clause = None
    beacons = None
    for element in invoice:
        if isinstance(element, contracts.Row):
            if data is None:
                data = [element]
            else:
                data.append(element)
        elif isinstance(element, contracts.Clause):
            if clause is None:
                clause = element
            else:
                clause += element
        elif isinstance(element, contracts.MetaElement):
            if beacons is None:
                beacons = []
            beacons.append(element)
    return data, clause, beacons


class WriteExecutor:
    def __init__(
        self,
        schema: Schema,
        connection: contracts.WriteAsyncConnectionContract,
    ):
        self._schema = schema
        self._connection = connection

    async def insert(
        self,
        namespace: contracts.Namespace,
        data: list[contracts.Row]
    ):
        table = self._schema.get_table(namespace)
        await self._connection.execute(
            sqlalchemy.insert(table()).values(
                [_.as_dict() for _ in data]
            )
        )

    async def update(
        self,
        namespace: contracts.Namespace,
        data: contracts.Row,
        clause: contracts.Clause
    ):
        table = self._schema.get_table(namespace)
        query = sqlalchemy.update(
            table()
        ).values(
            data.as_dict()
        ).where(
            FilterCompiler(schema=self._schema)(clause)
        )
        await self._connection.execute(query)

    async def delete(
        self,
        namespace: contracts.Namespace,
        clause: contracts.Clause
    ):
        table = self._schema.get_table(namespace)
        query = sqlalchemy.delete(
            table()
        ).where(
            FilterCompiler(schema=self._schema)(clause)
        )
        await self._connection.execute(query)


class WriteClient:
    def __init__(
        self,
        schema: Schema,
        connection: contracts.WriteAsyncConnectionContract,
        plugin_registry: contracts.PluginRegistry | None = None
    ):
        self._executor = WriteExecutor(
            schema=schema,
            connection=connection
        )
        self._plugin_registry = {}
        for beacon_type in (plugin_registry or ()):
            if plugin_class := plugin_registry.get(beacon_type):
                if contracts.WritePlugin in plugin_class.__mro__:
                    self._plugin_registry[beacon_type] = plugin_class(executor=self._executor)

    async def __call__(
        self,
        invoice: contracts.Invoice
    ):
        data, clause, beacons = invoice_separator(invoice)
        invoice = await self._apply_plugins(beacons, invoice)
        data, clause, beacons = invoice_separator(invoice)
        if data is None and clause is not None:
            await self._executor.delete(invoice.namespace, clause)
        elif data is not None and clause is None:
            await self._executor.insert(invoice.namespace, data)
        elif data is not None and clause is not None:
            for row in data:
                await self._executor.update(invoice.namespace, row, clause)

    async def _apply_plugins(
        self,
        beacons: collections.abc.Iterable[contracts.MetaElement],
        invoice: contracts.Invoice
    ):
        for beacon in beacons or ():
            if plugin := self._plugin_registry.get(type(beacon)):
                invoice = await plugin(invoice)
        return invoice
