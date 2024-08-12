import typing

from sqlalchemy.ext.asyncio import create_async_engine

from .definition import contracts


def get_engine(
    dsn: str,
    debug: bool = False,
    debug_level: typing.Literal['INFO', 'DEBUG'] = 'INFO',
    pool_size: int | None = None,
    max_overflow: int | None = None,
) -> contracts.AsyncEngineContract:
    params = dict(
        echo=debug_level if debug and debug_level == 'DEBUG' else debug,
        echo_pool=debug_level if debug and debug_level == 'DEBUG' else debug,
    )
    if pool_size:
        params['pool_size'] = pool_size
    if max_overflow:
        params['max_overflow'] = max_overflow
    return typing.cast(contracts.AsyncEngineContract, create_async_engine(dsn, **params))


async def get_connection(engine: contracts.AsyncEngineContract) -> contracts.AsyncConnectionContract:
    return typing.cast(contracts.AsyncConnectionContract, await engine.connect())


async def close_connection(
    connection: contracts.AsyncConnectionContract,
    context: contracts.ShutdownContext
):
    if connection:
        if context.get('exc_type'):
            await connection.rollback()
        else:
            await connection.commit()
        await connection.close()
