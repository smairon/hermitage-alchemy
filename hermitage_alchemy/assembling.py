import abc
import collections.abc
import operator
import typing

import sqlalchemy
import zodchy
from collections import deque

from hermitage.notation.default import (
    Bucket,
    Item,
    Total,
    Clause,
    Slice,
    ClauseExpression,
    AND,
    OR
)
from .configuration import (
    Schema,
    M2O,
    Address,
    Space,
    TOTAL_QUERY_FIELD
)


class ClauseCompiler(abc.ABC):
    def __init__(
        self,
        schema: Schema,
        space: Space | None = None
    ):
        self._schema = schema
        self._space = space

    def get_instance(self, space: Space) -> typing.Self:
        return type(self)(schema=self._schema, space=space)

    def __call__(self, expression: ClauseExpression) -> sqlalchemy.ClauseElement | None:
        if expression:
            result = self._compile(expression)
            return result

    @abc.abstractmethod
    def _compile(self, expression: ClauseExpression) -> sqlalchemy.ClauseElement: ...


class FilterCompiler(ClauseCompiler):
    @property
    def _operations(self):
        return {
            zodchy.codex.query.EQ: self._simple_clause(operator.eq),
            zodchy.codex.query.NE: self._simple_clause(operator.ne),
            zodchy.codex.query.LE: self._simple_clause(operator.le),
            zodchy.codex.query.LT: self._simple_clause(operator.lt),
            zodchy.codex.query.GE: self._simple_clause(operator.ge),
            zodchy.codex.query.GT: self._simple_clause(operator.gt),
            zodchy.codex.query.IS: lambda v: self._get_column(v).is_(v.operation.value),
            zodchy.codex.query.LIKE: self._like_clause,
            zodchy.codex.query.NOT: self._not_clause,
            zodchy.codex.query.SET: self._set_clause,
            zodchy.codex.query.RANGE: self._range_clause,
        }

    def _compile(self, expression: ClauseExpression) -> sqlalchemy.ClauseElement:
        stack = deque()
        for element in expression:
            if element is AND:
                a = stack.pop()
                b = stack.pop()
                stack.append(sqlalchemy.and_(a, b))
            elif element is OR:
                stack.append(sqlalchemy.or_(stack.pop(), stack.pop()))
            else:
                stack.append(self._operations[type(element.operation)](element))

        return stack.pop()

    def _get_column(self, clause: Clause) -> sqlalchemy.Column:
        return self._schema.get_column(Address(clause.name, self._space))

    def _not_clause(self, clause: Clause):
        if isinstance(clause.operation, zodchy.codex.query.IS):
            return self._get_column(clause).isnot(clause.operation.value)
        elif isinstance(clause.operation, zodchy.codex.query.EQ):
            return operator.ne(self._get_column(clause), clause.operation.value)
        elif isinstance(clause.operation, zodchy.codex.query.LIKE):
            return self._like_clause(
                Clause(clause.name, clause.operation),
                inversion=True
            )
        elif isinstance(clause.operation, zodchy.codex.query.SET):
            return self._set_clause(
                Clause(clause.name, clause.operation),
                inversion=True
            )
        else:
            return sqlalchemy.not_(self._compile(clause)),

    def _simple_clause(self, op):
        return lambda v: op(self._get_column(v), v.operation.value)

    def _logic_clause(self, op):
        return lambda v: op(self._compile(u) for u in v)

    def _like_clause(self, clause: Clause, inversion: bool = False):
        column = self._get_column(clause)
        operation = clause.operation
        value = f'%{operation.value}%'
        if operation.case_sensitive:
            return column.notlike(value) if inversion else column.like(value)
        else:
            return column.notilike(value) if inversion else column.ilike(value)

    def _set_clause(self, clause: Clause, inversion: bool = False):
        value = list(clause.operation.value)
        if inversion:
            return self._get_column(clause).notin_(value)
        else:
            return self._get_column(clause).in_(value)

    def _range_clause(self, clause: Clause):
        params = [
            Clause(clause.name, condition)
            for condition in clause.operation.value
            if condition is not None
        ]
        if len(params) > 1:
            return self._logic_clause(sqlalchemy.and_)(params)
        elif len(params) == 1:
            return params[0]


class OrderCompiler(ClauseCompiler):
    def __call__(self, clause: ClauseExpression) -> list[sqlalchemy.ClauseElement]:
        result = self._compile(clause)
        return result

    @property
    def _operations(self):
        return {
            zodchy.codex.query.ASC: sqlalchemy.asc,
            zodchy.codex.query.DESC: sqlalchemy.desc
        }

    def _compile(self, expression: ClauseExpression) -> list[sqlalchemy.ClauseElement]:
        stack = deque()
        result = []
        for element in expression or ():
            if element is AND:
                while stack:
                    result.append(stack.pop())
            else:
                result.append(
                    self._operations[type(element.operation)](
                        self._schema.get_column(Address(element.name, self._space))
                    )
                )
        return result


class Query:
    def __init__(
        self,
        schema: Schema
    ):
        self._schema = schema
        self._filter_compiler = FilterCompiler(self._schema)
        self._order_compiler = OrderCompiler(self._schema)
        self._aliases = {}
        self._select = []
        self._joins = []
        self._values = []
        self._filters = []
        self._orders = []
        self._limit = None
        self._offset = None

    def __call__(self, bucket: Bucket):
        self._process_bucket(bucket)
        return self._build_query(bucket)

    def _build_query(self, bucket: Bucket):
        if self._select:
            q = sqlalchemy.select(*self._select)
        elif self._values:
            if self._filters:
                q = sqlalchemy.update(self._schema.get_table(Space(bucket.name))).values(self._values[0])
            else:
                q = sqlalchemy.insert(self._schema.get_table(Space(bucket.name))).values(self._values)
        else:
            if self._filters:
                q = sqlalchemy.delete(self._schema.get_table(Space(bucket.name)))
            else:
                raise ValueError("Cannot determine operation type")

        for join in self._joins:
            q = q.join_from(*join, isouter=True)
        for filter_clause in self._filters:
            q = q.where(filter_clause)
        for order_clause in self._orders:
            q = q.order_by(order_clause)
        if self._limit:
            q = q.limit(self._limit)
        if self._offset:
            q = q.offset(self._offset)

        return q

    def _process_bucket(self, bucket: Bucket, parent: Space | None = None):
        bucket_space = Space(bucket.qualified_name)
        if parent:
            bucket_space = parent + bucket_space

        _filter_clause = None
        _order_clause = None
        _limit_clause = None
        _offset_clause = None
        for element in bucket:
            if isinstance(element, Bucket):
                element_space = bucket_space + Space(element.qualified_name)
                if isinstance(link := self._schema.get_link(bucket_space, element_space), M2O):
                    source_column = self._schema.get_column(Address(link.source_address.name, bucket_space))
                    target_column = self._schema.get_column(Address(link.target_address.name, element_space))
                    self._joins.append((
                        self._schema.get_table(bucket_space),
                        self._schema.get_table(element_space),
                        source_column == target_column
                    ))
                    self._process_bucket(element, bucket_space)
            elif isinstance(element, Item):
                _value = element()
                if isinstance(_value, str):
                    self._select.append(
                        self._schema.get_column(Address(_value, bucket_space))
                    )
                elif isinstance(_value, collections.abc.Mapping):
                    self._values.append(_value)
            elif isinstance(element, Total):
                self._select.append(
                    sqlalchemy.text(f"count(*) over () as {TOTAL_QUERY_FIELD}")
                )
            elif isinstance(element, Clause):
                if isinstance(element.operation, zodchy.codex.query.FilterBit):
                    _filter_clause = ClauseExpression(element) if _filter_clause is None else _filter_clause & element
                elif isinstance(element.operation, zodchy.codex.query.OrderBit):
                    _order_clause = ClauseExpression(element) if _order_clause is None else _order_clause & element
            elif isinstance(element, Slice):
                if isinstance(element.operation, zodchy.codex.query.Limit):
                    _limit_clause = element.operation.value
                elif isinstance(element.operation, zodchy.codex.query.Offset):
                    _offset_clause = element.operation.value
            elif isinstance(element, ClauseExpression):
                if isinstance(element[0].operation, zodchy.codex.query.FilterBit):
                    _filter_clause = element if _filter_clause is None else _filter_clause & element
                elif isinstance(element[0].operation, zodchy.codex.query.OrderBit):
                    _order_clause = element if _order_clause is None else _order_clause & element

        if _filter_clause:
            filter_compiler = self._filter_compiler.get_instance(bucket_space)
            self._filters.append(filter_compiler(_filter_clause))

        if _order_clause:
            order_compiler = self._order_compiler.get_instance(bucket_space)
            self._orders.extend(order_compiler(_order_clause))

        if _limit_clause:
            self._limit = _limit_clause

        if _offset_clause:
            self._offset = _offset_clause


class QueryBuilder:
    def __init__(self, schema: Schema):
        self._schema = schema

    def __call__(self, bucket: Bucket):
        return Query(schema=self._schema)(bucket)
