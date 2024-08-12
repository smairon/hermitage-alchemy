import typing
import collections.abc
import uuid
import datetime


def apply_decoders(value: str):
    decoders = (uuid.UUID, datetime.datetime.fromisoformat)
    result: typing.Any = value
    for _decoder in decoders:
        try:
            result = _decoder(result)
        except Exception:
            continue
    return result


def decode(data: collections.abc.Mapping):
    result = {}
    for k, v in data.items():
        if isinstance(v, collections.abc.Mapping):
            result[k] = decode(v)
        else:
            result[k] = apply_decoders(v)
    return result
