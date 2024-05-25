import collections.abc
import typing
import asyncio

import zodchy

from hermitage.notation import (
    Invoice,
    Bucket,
    Item,
    Clause,
    View
)
from ..configuration import Schema, O2M, M2M, Space, TOTAL_QUERY_FIELD
from ..assembling import QueryBuilder
from ..definition import contracts


class Squeezer:
    def __init__(
        self,
        separator: str = '__',
        is_collapse_none: bool = True
    ):
        super().__init__()
        self._separator = separator
        self._is_collapse_none = is_collapse_none

    def __call__(
        self,
        data: collections.abc.MutableMapping
    ):
        data = self._nestify(data)
        return self._collapse_none(data)

    def _split_rec(self, k, v, out):
        k, *rest = k.split(self._separator, 1)
        if rest:
            self._split_rec(rest[0], v, out.setdefault(k, {}))
        else:
            out[k] = v

    def _nestify(
        self,
        data: typing.Any
    ):
        if isinstance(data, str):
            return data
        elif isinstance(data, bytearray):
            return data
        elif isinstance(data, bytes):
            return data
        elif isinstance(data, collections.abc.Mapping):
            result = {}
            for k, v in data.items():
                self._split_rec(k, self._nestify(v), result)
            return result
        elif isinstance(data, collections.abc.Iterable):
            return [self._nestify(item) for item in data]
        else:
            return data

    def _collapse_none(self, data: collections.abc.MutableMapping):
        if self._is_collapse_none is False:
            return data
        result = {}
        for k, v in data.items():
            if isinstance(v, collections.abc.MutableMapping):
                v = self._collapse_none(v) if any(_ is not None for _ in v.values()) else None
            if isinstance(v, list):
                v = [self._collapse_none(_) for _ in v]
            result[k] = v
        return result


class ReadExecutor:
    def __init__(
        self,
        schema: Schema,
        connection: contracts.ReadAsyncConnectionContract,
        builder: QueryBuilder,
    ):
        self._schema = schema
        self._connection = connection
        self._builder = builder
        self._squeezer = Squeezer()

    async def __call__(
        self,
        bucket: Bucket,
        is_nested: bool = False
    ) -> View:
        nested_buckets = []
        # Detection of nested buckets and enrichment parent query with data for further nested calculations
        for element in bucket:
            if isinstance(element, Bucket):
                link = self._schema.get_link(
                    Space(f'{bucket.name}:{element.get_qua()}' if element.get_qua() else bucket.name),
                    Space(element.name)
                )
                if isinstance(link, O2M) or isinstance(link, M2M):
                    # Adding to parent bucket link key field
                    bucket += Item(link.source_address.name)
                    nested_buckets.append((element, link, element.substitution_name))

        # Build and run parent bucket
        query = self._builder(bucket)
        result = await self._connection.execute(query)
        bucket_data = result.mappings().fetchall()

        nested_indexed_data = {}
        # Nested buckets calculation
        for nested_bucket, link, substitution_name in nested_buckets:
            if isinstance(link, O2M):
                index_key = (substitution_name, link.source_address.name)
                nested_bucket += Item(link.target_address.name)
                nested_bucket += Clause(
                    link.target_address.name,
                    zodchy.codex.query.SET(*(e[link.source_address.name] for e in bucket_data))
                )
                nested_view = await self(nested_bucket, True)
                nested_indexed_data[index_key] = self._index_data(
                    data=nested_view.data,
                    key=link.target_address.name,
                    transformer=lambda r: {
                        k: v
                        for k, v in r.items()
                        if k != link.target_address.name
                    }
                )
            elif isinstance(link, M2M):
                index_key = (substitution_name, link.source_address.name)
                nested_bucket = Bucket(
                    link.interim_source_address.space.last,
                    Item(link.interim_source_address.name),
                    Clause(
                        link.interim_source_address.name,
                        zodchy.codex.query.SET(*(e[link.source_address.name] for e in bucket_data))
                    ),
                    nested_bucket + Item(link.target_address.name)
                )
                nested_view = await self(nested_bucket, True)
                nested_indexed_data[index_key] = self._index_data(
                    data=nested_view.data,
                    key=link.interim_source_address.name,
                    transformer=lambda r: {
                        k.replace(f'{substitution_name}__', ''): v
                        for k, v in r.items()
                        if k != link.interim_source_address.name
                    }
                )

        data = []
        meta = None
        for row in bucket_data:
            if is_nested and nested_indexed_data:
                row = dict(row)
            else:
                row = self._squeezer(row)
                if TOTAL_QUERY_FIELD in row:
                    if meta is None:
                        meta = {'total': row[TOTAL_QUERY_FIELD]}
                    del row[TOTAL_QUERY_FIELD]
            for k, v in nested_indexed_data.items():
                row[k[0]] = v.get(row.get(k[1]), [])
            data.append(row)

        return View(data=data, meta=meta)

    @staticmethod
    def _index_data(
        data: collections.abc.Iterable[collections.abc.Mapping],
        key: str,
        transformer: typing.Callable[[collections.abc.Mapping], collections.abc.Mapping] | None = None
    ):
        result = collections.defaultdict(list)
        for row in data:
            index = row[key]
            if transformer:
                row = transformer(row)
            result[index].append(row)
        return result


class ReadClient:
    def __init__(
        self,
        schema: Schema,
        connection: contracts.ReadAsyncConnectionContract
    ):
        self._executor = ReadExecutor(
            schema,
            connection,
            QueryBuilder(schema)
        )

    async def __call__(
        self,
        invoice: Invoice,
    ) -> View | list[View]:
        tasks = []
        async with asyncio.TaskGroup() as tg:
            for bucket in invoice:
                tasks.append(tg.create_task(self._executor(bucket)))
        result = [_.result() for _ in tasks]
        return result if len(result) > 1 else result[0]
