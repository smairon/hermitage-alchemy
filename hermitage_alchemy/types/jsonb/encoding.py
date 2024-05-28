import collections.abc
import typing
import uuid
import datetime
from functools import singledispatch


@singledispatch
def encode_rule(value: typing.Any):
    return value


@encode_rule.register(uuid.UUID)
def encode_uuid(value: uuid.UUID):
    return str(value)


@encode_rule.register(datetime.datetime)
def encode_datetime(value: datetime.datetime):
    return value.isoformat()


def encode(data: collections.abc.Mapping):
    result = {}
    for k, v in data.items():
        if isinstance(v, collections.abc.Mapping):
            result[k] = encode(v)
        else:
            result[k] = encode_rule(v)
    return result
