from src import models, db
from src.db import Database


class Catalog:
    def __init__(self, items: list):
        self._items = items

    def find(self, key: int):
        for item in self._items:
            if item.Id == key:
                return item
        return None

    def keys(self) -> list[int]:
        return [item.Id for item in self._items]

    def __iter__(self):
        return iter(self._items)


def get_test_catalog(conn: Database) -> Catalog:
    sql = """
SELECT
  LabTests.*,
  Labs.Name AS LabName 
FROM
  [Catalog].LabTests
  INNER JOIN [Catalog].Labs ON LabTests.PerformingLabId = Labs.Id 
WHERE
  LabTests.IsActive = 1 
  AND Labs.IsActive = 1    
    """
    rows = conn.fetch_all(sql)
    return Catalog([models.LabTest(**row) for row in rows])


def get_staff_catalog(conn: Database) -> Catalog:
    sql = """
SELECT
  *
FROM
  Staff.Users
WHERE
  IsActive = 1 
    """
    rows = conn.fetch_all(sql)
    return Catalog([models.User(**row) for row in rows])


def invoice_tests(invoice_id: int, db_: Database) -> list[models.OrderedLabTest]:
    sql = """
SELECT
  ot.*,
  tst.ShortName AS TestName 
FROM
  PROE.PatientLabOrders AS ord
  INNER JOIN PROE.OrderedTests AS ot ON ord.InvoiceId = ot.InvoiceId
  INNER JOIN [Catalog].LabTests AS tst ON ot.LabTestId = tst.Id 
WHERE
  ot.IsCancelled = 0 
  AND ord.InvoiceId = ?    
    """
    rows = db_.fetch_all(sql, invoice_id)
    return [models.OrderedLabTest(**row) for row in rows]


def invoice_items(invoice_id: int, db_: Database) -> list[models.OrderedBillableItem]:
    sql = """
SELECT
  obi.*,
  bi.Name AS BillableItemName 
FROM
  PROE.PatientLabOrders AS ord
  INNER JOIN PROE.OrderedBillableItems AS obi ON ord.InvoiceId = obi.InvoiceId
  INNER JOIN [Catalog].BillableItems AS bi ON obi.BillableItemId = bi.Id 
WHERE
  ord.InvoiceId = ? 
  AND obi.IsCancelled = 0
    """
    rows = db_.fetch_all(sql, invoice_id)
    return [models.OrderedBillableItem(**row) for row in rows]


class OrderContext:
    order: models.LabOrder
    tests: list[models.OrderedLabTest] = []
    items: list[models.OrderedBillableItem] = []

    def __init__(self, order: models.LabOrder):
        self.order = order

    def scan(self, db_: Database):
        self.tests = invoice_tests(self.order.InvoiceId, db_)
        self.items = invoice_items(self.order.InvoiceId, db_)

    def filter_tests(self, catalog: Catalog):
        self.tests = [t for t in self.tests if t.LabTestId in catalog.keys()]
