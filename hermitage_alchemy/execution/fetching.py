import collections.abc
import functools
import typing

import hermitage

from ..configuration import Schema
from ..assembling import QueryBuilder
from ..definition import contracts

ResultDataType = collections.abc.Iterable[collections.abc.Mapping]


class ReadClient:
    def __init__(
        self,
        schema: Schema,
        connection: contracts.ReadAsyncConnectionContract,
        plugin_registry: contracts.PluginRegistry | None = None
    ):
        self._schema = schema
        self._connection = connection
        self._plugin_registry = plugin_registry

    async def __call__(
        self,
        invoice: contracts.Invoice,
    ) -> contracts.View:
        data, plugins = await self._launch_invoice(invoice)

        meta = {}
        for plugin in plugins:
            meta |= plugin.get_result()

        return contracts.View(
            data=data,
            meta=meta or None
        )

    def _get_links(self, invoice: contracts.Invoice) -> dict[contracts.Item, contracts.Link]:
        result = {}
        for item in list(filter(lambda x: x.invoice is not None, invoice.items)):
            if link := self._schema.get_o2m(item.invoice.namespace):
                result[item] = link
                invoice += contracts.Item(str(link.source_column), link.source_column)
            elif link := self._schema.get_m2m(item.invoice.namespace):
                result[item] = link
        return result

    async def _launch_invoice(
        self,
        invoice: contracts.Invoice
    ) -> tuple[ResultDataType, list[contracts.Plugin]]:
        links = self._get_links(invoice)

        query_builder = QueryBuilder(
            schema=self._schema,
            invoice=invoice,
            plugins=self._get_plugins()
        )

        result = await self._connection.execute(query_builder.query)
        data = result.mappings().fetchall()

        plugins = []
        mappers = [_nestify, _collapse_none]
        for element in invoice.meta:
            if self._plugin_registry and (plugin_class := self._plugin_registry.get(type(element))):
                plugin = plugin_class(data)
                plugins.append(plugin)
                mappers.extend(plugin.get_mappers())

        data = [
            functools.reduce(lambda v, f: f(v), mappers, dict(row))
            for row in data
        ]

        for item, link in links.items():
            data = await self._attach_o2m(item, link, data)
            data = await self._attach_m2m(item, link, data)

        return data, plugins

    async def _attach_m2m(
        self,
        item: contracts.Item,
        link: contracts.Link,
        data: collections.abc.MutableSequence[collections.abc.MutableMapping]
    ):
        if not isinstance(link, contracts.M2M):
            return data
        nested_invoice = contracts.Invoice(
            str(link.interim_table),
            str(link.interim_source_column),
            contracts.Item(
                item.name,
                contracts.Invoice(
                    self._schema.get_fk_table_alias(link.interim_target_column),
                    *item.invoice
                )
            )
        )
        nested_invoice += contracts.Clause(
            link.interim_source_column,
            hermitage.query.SET(*(r.get(str(link.source_column)) for r in data))
        )
        nested_result = await self._launch_invoice(nested_invoice)
        index = collections.defaultdict(list)
        for row in nested_result[0]:
            index[row[str(link.interim_source_column)]].append(
                {k.replace(f'{item.name}__', ''): v for k, v in row.items() if k.startswith(item.name)}
            )
        for row in data:
            row[item.name] = index.get(row[str(link.source_column)], [])
        return data

    async def _attach_o2m(
        self,
        item: contracts.Item,
        link: contracts.Link,
        data: collections.abc.MutableSequence[collections.abc.MutableMapping]
    ):
        if not isinstance(link, contracts.O2M):
            return data
        original_items = set(
            item.name for item in item.invoice.items if item.address is not None
        )
        nested_invoice = item.invoice
        nested_invoice += contracts.Item(
            str(link.target_column),
            link.target_column
        )
        nested_invoice += contracts.Clause(
            link.target_column,
            hermitage.query.SET(*(r.get(str(link.source_column)) for r in data))
        )
        nested_result = await self._launch_invoice(nested_invoice)
        index = collections.defaultdict(list)
        for row in nested_result[0]:
            index[row[str(link.target_column)]].append(
                {k: v for k, v in row.items() if k in original_items}
            )
        for row in data:
            row[item.name] = index.get(row[str(link.source_column)], [])
        return data

    def _get_plugins(self):
        if self._plugin_registry:
            return {
                k: p.get_query_builder_plugin
                for k, p in self._plugin_registry.items()
                if contracts.ReadPlugin in p.__mro__ and p.get_query_builder_plugin is not None
            }


def _nestify(data: typing.Any):
    if isinstance(data, str):
        return data
    elif isinstance(data, bytearray):
        return data
    elif isinstance(data, bytes):
        return data
    elif isinstance(data, collections.abc.Mapping):
        result = {}
        for k, v in data.items():
            _split_rec(k, _nestify(v), result)
        return result
    elif isinstance(data, collections.abc.Iterable):
        return [_nestify(item) for item in data]
    else:
        return data


def _split_rec(k, v, out):
    k, *rest = k.split('__', 1)
    if rest:
        _split_rec(rest[0], v, out.setdefault(k, {}))
    else:
        out[k] = v


def _collapse_none(data: collections.abc.Mapping):
    result = {}
    for k, v in data.items():
        if isinstance(v, collections.abc.Mapping):
            v = _collapse_none(v) if any(_ is not None for _ in v.values()) else None
        if isinstance(v, list):
            v = [_collapse_none(_) for _ in v]
        result[k] = v
    return result
