import datetime

from src import models, utils
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


def fetch_invoices(dt: datetime.date, db_: Database) -> list[models.LabOrder]:
    sql = """
SELECT
    ord.*,
    COALESCE(ref.FullName, ord.ReferrerCustomName, '') AS ReferrerCustomName
FROM
    PROE.PatientLabOrders AS ord
    LEFT JOIN [Catalog].Referrers AS ref ON ord.ReferrerId = ref.Id 
WHERE
    CAST (ord.OrderDateTime AS DATE) = ?        
    """
    rows = db_.fetch_all(sql, dt)
    return [models.LabOrder(**row) for row in rows]


def fetch_invoices_after(
    dt: datetime.date, last_id: int, db_: Database
) -> list[models.LabOrder]:
    sql = """
SELECT
    ord.*,
    COALESCE(ref.FullName, ord.ReferrerCustomName, '') AS ReferrerCustomName
FROM
    PROE.PatientLabOrders AS ord
    LEFT JOIN [Catalog].Referrers AS ref ON ord.ReferrerId = ref.Id 
WHERE
    InvoiceId > ? AND
    CAST (ord.OrderDateTime AS DATE) = ?        
    """
    rows = db_.fetch_all(sql, last_id, dt)
    return [models.LabOrder(**row) for row in rows]


def get_test_catalog(db_: Database) -> Catalog:
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
    rows = db_.fetch_all(sql)
    return Catalog([models.LabTest(**row) for row in rows])


def get_staff_catalog(db_: Database) -> Catalog:
    sql = """
SELECT
  *
FROM
  Staff.Users
WHERE
  IsActive = 1 
    """
    rows = db_.fetch_all(sql)
    return Catalog([models.User(**row) for row in rows])


def invoice_tests(invoice_id: int, db_: Database) -> list[models.OrderedLabTest]:
    sql = """
SELECT
  ot.*,
  tst.ShortName AS TestName,
  tst.PerformingLabId AS LabId 
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


def invoice_bundles(invoice_id: int, db_: Database) -> list[models.ResultBundle]:
    sql = """
SELECT
  * 
FROM
  TestResults.ResultBundles 
WHERE
  InvoiceId = ? 
  AND IsActive = 1    
    """
    rows = db_.fetch_all(sql, invoice_id)
    return [models.ResultBundle(**row) for row in rows]


def invoice_master(invoice_id: int, db_: Database) -> models.Invoice:
    sql = "SELECT TOP 1 * FROM Finances.InvoiceMaster AS inv WHERE InvoiceId = ?"
    return models.Invoice(**db_.fetch(sql, invoice_id))


def invoice_primal(invoice_id: int, db_: Database) -> models.Invoice:
    sql = "SELECT TOP 1 * FROM Finances.InvoicePrimal AS inv WHERE InvoiceId = ?"
    return models.Invoice(**db_.fetch(sql, invoice_id))


def invoice_transactions(
    invoice_id: int, db_: Database
) -> list[models.InvoiceTransaction]:
    sql = "SELECT TOP 1 * FROM Finances.InvoiceTransactions AS inv WHERE InvoiceId = ?"
    rows = db_.fetch_all(sql, invoice_id)
    return [models.InvoiceTransaction(**row) for row in rows]


def last_src_invoice_id(dt: datetime.date, db_: Database) -> int | None:
    sql = "SELECT MAX(SourceInvoiceId) AS _id_ FROM PROE.PatientLabOrders WHERE CAST (OrderDateTime AS DATE) = ?"
    return db_.fetch_scalar(sql, "_id_", dt)


def insert_order(order: models.LabOrder, db_: Database) -> bool:
    sql = "SELECT COUNT(*) AS C FROM PROE.PatientLabOrders WHERE SourceInvoiceId = ?"
    if db_.fetch_scalar(sql, "C", order.InvoiceId) > 0:
        utils.croak(f"Skipping #{order.InvoiceId}. already exists")
        return False
    # sql = 'EXEC PROE.SP_ShadowCreateNewLabOrderFull(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,)'
    sql = """
  INSERT INTO PROE.PatientLabOrders(
     SourceInvoiceId,
     OrderId,
     OrderDateTime,
     WorkflowStage,
     LastModified,
     IsCancelled,
     ReferrerId,
     DisallowReferral,
     OrderingUserId,
     WorkShiftId,
     RequestingLabId,
     Title,
     FirstName,
     LastName,
     Sex,
     Age,
     DoB,
     PhoneNumber,
     EmailAddress,
     EmailTestResults,
     IsReferrerUnknown,
     ReferrerCustomName,
     OrderNotes,
     WebAccessToken,
     RegisteredMemberId,
     SubOrderTrackingId,
     IsExternalSubOrder,
     MirrorFlag
  )
  VALUES
   (
     ?,
     ?,
     ?,
     ?,
     ?,
     0,
     NULL, -- @refId,
     0, -- @refDisallow,
     ?,
     NULL, -- @shiftId,
     NULL, -- @reqLabId,
     ?,
     ?,
     ?,
     ?,
     ?,
     ?,
     ?,
     NULL, -- @email,
     0, -- @emailResult,
     1, -- @refUnk,
     ?, -- @refCustName,
     NULL, -- @notes,
     ?, -- @webToken,
     NULL, -- @regMemberId,
     NULL, -- @subTrackingId,
     0, -- @subIsExternal,
     0)    
    """
    db_.execute(
        sql,
        order.InvoiceId,
        order.OrderId,
        order.OrderDateTime,
        order.WorkflowStage,
        order.OrderDateTime,
        order.OrderingUserId,
        order.Title,
        order.FirstName,
        order.LastName,
        order.Sex,
        order.Age,
        order.DoB,
        order.PhoneNumber,
        order.ReferrerCustomName,
        order.WebAccessToken,
    )
    return True
