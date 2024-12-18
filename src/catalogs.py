from src import models
from src.db import Database
from src.dal import (
    invoice_tests,
    invoice_items,
    invoice_bundles,
    invoice_master,
    invoice_primal,
    invoice_transactions,
    Catalog,
)


class OrderContext:
    order: models.LabOrder
    tests: list[models.OrderedLabTest] = []
    items: list[models.OrderedBillableItem] = []
    bundles: list[models.ResultBundle] = []
    master: models.Invoice
    primal: models.Invoice
    transactions: list[models.InvoiceTransaction] = []

    def __init__(self, order: models.LabOrder):
        self.order = order

    def scan(self, db_: Database):
        self.tests = invoice_tests(self.order.InvoiceId, db_)
        self.items = invoice_items(self.order.InvoiceId, db_)
        self.bundles = invoice_bundles(self.order.InvoiceId, db_)
        self.master = invoice_master(self.order.InvoiceId, db_)
        self.primal = invoice_primal(self.order.InvoiceId, db_)
        self.transactions = invoice_transactions(self.order.InvoiceId, db_)

    def sanitize_tests(self, catalog: Catalog):
        tests = [t for t in self.tests if t.LabTestId in catalog.keys()]
        self.tests = []
        for test in tests:
            for bundle in self.bundles:
                if bundle.LabId == test.LabId:
                    test.ResultBundleId = bundle.Id
            self.tests.append(test)
