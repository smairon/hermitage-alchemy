import typing
import asyncio

from hermitage.notation import (
    Invoice,
    Bucket
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
        query = self._builder(bucket)
        result = await self._connection.execute(query)
        return result


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
