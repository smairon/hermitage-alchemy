import collections.abc
import uuid
import datetime


def apply_decoders(value: str):
    decoders = (uuid.UUID, datetime.datetime.fromisoformat)
    for _decoder in decoders:
        try:
            value = _decoder(value)
        except Exception:
            continue
    return value


def decode(data: collections.abc.Mapping):
    result = {}
    for k, v in data.items():
        if isinstance(v, collections.abc.Mapping):
            result[k] = decode(v)
        else:
            result[k] = apply_decoders(v)
    return result
