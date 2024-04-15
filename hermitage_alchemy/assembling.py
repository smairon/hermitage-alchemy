import abc
import collections.abc
import operator
import functools
from collections import deque

import sqlalchemy
import hermitage

from .definition import contracts
from .configuration import Schema


class ClauseCompiler(abc.ABC):
    def __init__(
        self,
        schema: Schema,
    ):
        self._schema = schema

    def __call__(self, condition: contracts.Clause) -> sqlalchemy.ClauseElement | None:
        if condition:
            result = self._compile(condition)
            return result

    @abc.abstractmethod
    def _compile(self, clause: contracts.Clause) -> sqlalchemy.ClauseElement: ...


class FilterCompiler(ClauseCompiler):
    @property
    def _operations(self):
        return {
            hermitage.query.EQ: self._simple_clause(operator.eq),
            hermitage.query.NE: self._simple_clause(operator.ne),
            hermitage.query.LE: self._simple_clause(operator.le),
            hermitage.query.LT: self._simple_clause(operator.lt),
            hermitage.query.GE: self._simple_clause(operator.ge),
            hermitage.query.GT: self._simple_clause(operator.gt),
            hermitage.query.IS: lambda v: self._get_column(v).is_(v.operation.value),
            hermitage.query.LIKE: self._like_clause,
            hermitage.query.NOT: self._not_clause,
            hermitage.query.SET: self._set_clause,
            hermitage.query.RANGE: self._range_clause,
        }

    def _compile(self, clause: contracts.Clause) -> sqlalchemy.ClauseElement:
        if clause.address:
            return self._operations[type(clause.operation)](clause)

        stack = deque()
        for element in clause:
            if element is contracts.LogicalOperator.AND:
                a = stack.pop()
                b = stack.pop()
                stack.append(sqlalchemy.and_(a, b))
            elif element is contracts.LogicalOperator.OR:
                stack.append(sqlalchemy.or_(stack.pop(), stack.pop()))
            else:
                stack.append(self._operations[type(element.operation)](element))

        return stack.pop()

    def _get_column(self, clause: contracts.Clause) -> sqlalchemy.Column:
        return self._schema.get_column(clause)

    def _not_clause(self, clause: contracts.Clause):
        if isinstance(clause.operation, hermitage.query.IS):
            return self._get_column(clause).isnot(clause.operation.value)
        elif isinstance(clause.operation, hermitage.query.EQ):
            return operator.ne(self._get_column(clause), clause.operation.value)
        elif isinstance(clause.operation, hermitage.query.LIKE):
            return self._like_clause(
                contracts.Clause(clause.address, clause.operation),
                inversion=True
            )
        elif isinstance(clause.operation, hermitage.query.SET):
            return self._set_clause(
                contracts.Clause(clause.address, clause.operation),
                inversion=True
            )
        else:
            return sqlalchemy.not_(self._compile(clause)),

    def _simple_clause(self, op):
        return lambda v: op(self._get_column(v), v.operation.value)

    def _logic_clause(self, op):
        return lambda v: op(self._compile(u) for u in v)

    def _like_clause(self, clause: contracts.Clause, inversion: bool = False):
        column = self._get_column(clause)
        operation = clause.operation
        value = f'%{operation.value}%'
        if operation.case_sensitive:
            return column.notlike(value) if inversion else column.like(value)
        else:
            return column.notilike(value) if inversion else column.ilike(value)

    def _set_clause(self, clause: contracts.Clause, inversion: bool = False):
        value = list(clause.operation.value)
        if inversion:
            return self._get_column(clause).notin_(value)
        else:
            return self._get_column(clause).in_(value)

    def _range_clause(self, clause: contracts.Clause):
        params = [
            contracts.Clause(clause.address, condition).set_namespace(clause.namespace)
            for condition in clause.operation.value
            if condition is not None
        ]
        if len(params) > 1:
            return self._logic_clause(sqlalchemy.and_)(params)
        elif len(params) == 1:
            return params[0]


class OrderCompiler(ClauseCompiler):
    def __call__(self, condition: contracts.Clause) -> list[sqlalchemy.ClauseElement]:
        result = self._compile(condition)
        return result

    @property
    def _operations(self):
        return {
            hermitage.query.ASC: sqlalchemy.asc,
            hermitage.query.DESC: sqlalchemy.desc
        }

    def _compile(self, clause: contracts.Clause) -> list[sqlalchemy.ClauseElement]:
        stack = deque()
        result = []
        for element in clause or ():
            if element is contracts.LogicalOperator.AND:
                while stack:
                    result.append(stack.pop())
            else:
                result.append(
                    self._operations[type(element.operation)](
                        self._schema.get_column(element)
                    )
                )
        return result


class QueryBuilder:
    def __init__(
        self,
        invoice: contracts.Invoice,
        schema: Schema,
        plugins: collections.abc.Mapping[type[contracts.MetaElement], collections.abc.Callable] | None = None
    ):
        self._schema = schema
        self._plugins = plugins
        self._columns = []
        self._joins = []
        self._clauses = invoice.clauses
        self._filter = FilterCompiler(self._schema)(self._filter_clause)
        self._order = OrderCompiler(self._schema)(self._order_clause)
        self._limit = self._limit_clause.operation.value if self._limit_clause else None
        self._offset = self._offset_clause.operation.value if self._offset_clause else None
        self._parse_invoice(invoice)
        self._apply_plugins(invoice)

    @functools.cached_property
    def columns(self) -> list[sqlalchemy.Column]:
        return self._columns

    @functools.cached_property
    def filter(self) -> sqlalchemy.ClauseElement | None:
        return self._filter

    @functools.cached_property
    def order(self) -> list[sqlalchemy.ClauseElement] | None:
        return self._order

    @functools.cached_property
    def limit(self) -> int | None:
        return self._limit

    @functools.cached_property
    def offset(self) -> int | None:
        return self._offset

    @functools.cached_property
    def query(self):
        query = sqlalchemy.select(*self._columns)
        if self._joins:
            for join in self._joins:
                query = query.join_from(*join, isouter=True)
        if self.filter is not None:
            query = query.where(self.filter)
        for condition in self.order or ():
            query = query.order_by(condition)
        if self.limit:
            query = query.limit(self.limit)
        if self.offset:
            query = query.offset(self.offset)
        return query

    def _parse_invoice(
        self,
        invoice: contracts.Invoice,
        prefix: str | None = None
    ):
        table = self._schema.get_table(invoice.namespace)

        for item in filter(lambda x: x.address is not None, invoice.items):
            self._columns.append(
                table(
                    column_address=item.address,
                    column_alias=f'{prefix}__{item.name}' if prefix else item.name
                )
            )

        _joins_index = set()
        for nested_item in filter(lambda x: x.invoice is not None, invoice.items):
            nested_invoice = nested_item.invoice
            nested_namespace = invoice.namespace + nested_invoice.namespace
            if link := self._schema.get_m2o(nested_namespace):
                self._joins.append((
                    link.source_table(),
                    link.target_table(),
                    link.source_table(link.source_column) == link.target_table(link.target_column)
                ))
                self._parse_invoice(
                    invoice=contracts.Invoice(nested_namespace, *nested_invoice),
                    prefix=nested_item.name
                )
                _joins_index.add(str(link))

        for clause in filter(
            lambda x: hermitage.query.FilterBit in x.kind.__mro__ and len(x.namespace) > 1,
            invoice.clauses
        ):
            if link := self._schema.get_m2o(clause.namespace):
                if str(link) not in _joins_index:
                    self._joins.append((
                        link.source_table(),
                        link.target_table(),
                        link.source_table(link.source_column) == link.target_table(link.target_column)
                    ))

    def _apply_plugins(self, invoice: contracts.Invoice):
        for item in invoice.meta:
            if self._plugins and (plugin := self._plugins.get(type(item))):
                self._columns, self._filter, self._order, self._limit, self._offset = plugin()(
                    item,
                    self._columns,
                    self._filter,
                    self._order,
                    self._limit,
                    self._offset
                )

    @functools.cached_property
    def _filter_clause(self) -> contracts.Clause | None:
        clauses = filter(lambda x: hermitage.query.FilterBit in x.kind.__mro__, self._clauses)
        try:
            result = functools.reduce(operator.and_, clauses)
            return result
        except TypeError:
            return

    @functools.cached_property
    def _limit_clause(self) -> contracts.Clause | None:
        for clause in filter(lambda x: isinstance(x.operation, hermitage.query.Limit), self._clauses):
            return clause

    @functools.cached_property
    def _offset_clause(self) -> contracts.Clause | None:
        for clause in filter(lambda x: isinstance(x.operation, hermitage.query.Offset), self._clauses):
            return clause

    @functools.cached_property
    def _order_clause(self) -> contracts.Clause | None:
        clauses = filter(lambda x: hermitage.query.OrderBit in x.kind.__mro__, self._clauses)
        try:
            return functools.reduce(operator.and_, clauses)
        except TypeError:
            return
