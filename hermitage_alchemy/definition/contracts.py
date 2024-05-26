import typing

import hermitage
import sqlalchemy.ext.asyncio

AsyncEngineContract = sqlalchemy.ext.asyncio.AsyncEngine
AsyncConnectionContract = sqlalchemy.ext.asyncio.AsyncConnection


class ReadAsyncConnectionContract(AsyncConnectionContract):
    pass


class WriteAsyncConnectionContract(AsyncConnectionContract):
    pass


class ShutdownContext(typing.TypedDict):
    exc_type: type[Exception]
    exc_val: Exception


class ReadClientContract(typing.Protocol):
    async def __call__(self, invoice: hermitage.notation.Invoice) -> hermitage.notation.View: ...


class WriteClientContract(typing.Protocol):
    async def __call__(self, invoice: hermitage.notation.Invoice) -> hermitage.notation.View | None: ...
