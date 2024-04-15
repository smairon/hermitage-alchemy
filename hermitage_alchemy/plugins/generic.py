import abc

from ..execution.storing import WriteExecutor
from ..definition import contracts


class WritePlugin(contracts.WritePlugin, abc.ABC):
    def __init__(
        self,
        executor: WriteExecutor
    ):
        self._executor = executor
