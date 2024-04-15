class FilterException(Exception):
    def __init__(
        self,
        field_name: str,
        field_value: str,
        message: str
    ):
        self.field_name = field_name
        self.field_value = field_value
        super().__init__(f'{field_name}: {message} with value {field_value}')


class ColumnNotFound(Exception):
    def __init__(
        self,
        table_name: str,
        column_name: str
    ):
        self.table_name = table_name
        self.column_name = column_name
        super().__init__(f'{column_name} not found in table {table_name}')


class RangeException(Exception):
    pass


class TypeMismatch(Exception):
    pass
