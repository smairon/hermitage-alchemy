import typing

from sqlalchemy.ext.asyncio import create_async_engine

from .definition import contracts


def get_engine(dsn: str) -> contracts.AsyncEngineContract:
    return typing.cast(
        contracts.AsyncEngineContract,
        create_async_engine(
            url=dsn,
            echo=True,
        )
    )


async def get_connection(engine: contracts.AsyncEngineContract) -> contracts.AsyncConnectionContract:
    return typing.cast(contracts.AsyncConnectionContract, await engine.connect())


async def close_connection(
    connection: contracts.AsyncConnectionContract,
    context: contracts.ShutdownContext
) -> typing.NoReturn:
    if connection:
        if context.get('exc_type'):
            await connection.rollback()
        else:
            await connection.commit()
        await connection.close()
