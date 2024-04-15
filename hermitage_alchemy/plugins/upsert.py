from ..definition import contracts
from .generic import WritePlugin
from ..execution.storing import invoice_separator


class Beacon(contracts.MetaElement):
    def __init__(self, clause: contracts.Clause):
        super().__init__()
        self.clause = clause


class UpsertPlugin(WritePlugin):
    async def __call__(self, invoice: contracts.Invoice) -> contracts.Invoice:
        data, clause, beacons = invoice_separator(invoice)
        beacon = next(iter(filter(lambda x: isinstance(x, Beacon), beacons)))
        await self._executor.delete(
            invoice.namespace,
            beacon.clause.set_namespace(invoice.namespace)
        )
        return contracts.Invoice(
            invoice.namespace,
            *(element for element in invoice.elements if not isinstance(element, contracts.Clause))
        )

    @classmethod
    def get_beacon(cls) -> type[Beacon]:
        return Beacon
