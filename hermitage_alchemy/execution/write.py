import typing
import asyncio

from hermitage.notation.default import (
    Invoice,
    Bucket,
    Upsert,
    MetaElement
)
from ..configuration import Schema
from ..assembling import QueryBuilder
from ..definition import contracts


class WriteExecutor:
    def __init__(
        self,
        schema: Schema,
        connection: contracts.WriteAsyncConnectionContract,
        builder: QueryBuilder
    ):
        self._schema = schema
        self._connection = connection
        self._builder = builder

    async def __call__(self, bucket: Bucket):
        await self._apply_upsert(bucket)
        await self._process_bucket(bucket)

    async def _process_bucket(self, bucket: Bucket):
        await self._connection.execute(self._builder(bucket))

    async def _apply_upsert(self, bucket: Bucket):
        if upsert := typing.cast(self._search_meta(bucket, Upsert), Upsert):
            query = self._builder(
                Bucket(
                    bucket.name,
                    upsert.clause
                )
            )
            await self._connection.execute(query)

    @staticmethod
    def _search_meta(haystack: Bucket, needle: type[MetaElement]) -> MetaElement | None:
        for element in haystack:
            if isinstance(element, needle):
                return element


class WriteClient:
    def __init__(
        self,
        schema: Schema,
        connection: contracts.WriteAsyncConnectionContract,
    ):
        self._executor = WriteExecutor(
            schema,
            connection,
            QueryBuilder(schema)
        )

    async def __call__(
        self,
        invoice: Invoice
    ) -> typing.NoReturn:
        tasks = []
        async with asyncio.TaskGroup() as tg:
            for bucket in invoice:
                tasks.append(tg.create_task(self._executor(bucket)))
