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
ORDER BY
    ord.InvoiceId            
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
ORDER BY
    ord.InvoiceId
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


def purge_order_chain(invoice_id: int, db_: Database):
    db_.execute("EXEC PROE.SP_ShadowPurgeLabOrderChain ?", invoice_id)


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
     ?,
     NULL, -- @refId,
     0, -- @refDisallow,
     ?,
     ?, -- @shiftId,
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
        order.LastModified,
        order.IsCancelled,
        order.OrderingUserId,
        order.WorkShiftId,
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
    db_.commit()
    return True


def shadow_id_for_source_id(source_id: int, db_: Database) -> int | None:
    sql = "SELECT InvoiceId FROM PROE.PatientLabOrders WHERE SourceInvoiceId = ?"
    return db_.fetch_scalar(sql, "InvoiceId", source_id)


def insert_master(invoice_id: int, inv: models.Invoice, db_: Database):
    sql = """
INSERT INTO Finances.InvoiceMaster(
   InvoiceId
  ,DateCreated
  ,PaymentStatus
  ,GrossPayable
  ,DiscountAmount
  ,TaxAmount
  ,SurchargeAmount
  ,NetPayable
  ,PaidAmount
  ,DueAmount
  ,RefundAmount
  ,PaidUpReferral
) VALUES (
   ?   -- InvoiceId - bigint
  ,? -- DateCreated - smalldatetime
  ,?   -- PaymentStatus - tinyint
  ,?   -- GrossPayable - money
  ,?   -- DiscountAmount - money
  ,?   -- TaxAmount - money
  ,?   -- SurchargeAmount - money
  ,?   -- NetPayable - money
  ,?   -- PaidAmount - money
  ,?   -- DueAmount - money
  ,?   -- RefundAmount - money
  ,0   -- PaidUpReferral - money
)    
    """
    db_.execute(
        sql,
        invoice_id,
        inv.DateCreated,
        inv.PaymentStatus,
        inv.GrossPayable,
        inv.DiscountAmount,
        inv.TaxAmount,
        inv.SurchargeAmount,
        inv.NetPayable,
        inv.PaidAmount,
        inv.DueAmount,
        inv.RefundAmount,
    )


def insert_primal(invoice_id: int, inv: models.Invoice, db_: Database):
    sql = """
INSERT INTO Finances.InvoicePrimal(
   InvoiceId
  ,DateCreated
  ,GrossPayable
  ,DiscountAmount
  ,TaxAmount
  ,SurchargeAmount
  ,NetPayable
  ,PaidAmount
  ,DueAmount
  ,RefundAmount
) VALUES (
   ?   -- InvoiceId - bigint
  ,? -- DateCreated - smalldatetime
  ,?   -- GrossPayable - money
  ,?   -- DiscountAmount - money
  ,?   -- TaxAmount - money
  ,?   -- SurchargeAmount - money
  ,?   -- NetPayable - money
  ,?   -- PaidAmount - money
  ,?   -- DueAmount - money
  ,?   -- RefundAmount - money
)    
    """
    db_.execute(
        sql,
        invoice_id,
        inv.DateCreated,
        inv.GrossPayable,
        inv.DiscountAmount,
        inv.TaxAmount,
        inv.SurchargeAmount,
        inv.NetPayable,
        inv.PaidAmount,
        inv.DueAmount,
        inv.RefundAmount,
    )


def insert_transactions(
    invoice_id: int,
    transactions: list[models.InvoiceTransaction],
    shift_id: int,
    db_: Database,
):
    sql = """
INSERT INTO Finances.[InvoiceTransactions](
   InvoiceId
  ,PerformingUserId
  ,WorkShiftId
  ,AuthorizingUserId
  ,TxTime
  ,TxType
  ,TxFlag
  ,TxAmount
  ,UserIpAddress
  ,UserRemarks
  ,NonCashAmount
  ,PaymentMethod
  ,PaymentSource
  ,PaymentReference
) VALUES (
   ?   -- InvoiceId - bigint
  ,? -- PerformingUserId - smallint
  ,? -- WorkShiftId - int
  ,NULL -- AuthorizingUserId - smallint
  ,? -- TxTime - smalldatetime
  ,?   -- TxType - tinyint
  ,?   -- TxFlag - tinyint
  ,?   -- TxAmount - money
  ,NULL -- UserIpAddress - int
  ,NULL -- UserRemarks - varchar(160)
  ,?   -- NonCashAmount - money
  ,?   -- PaymentMethod - tinyint
  ,NULL -- PaymentSource - text
  ,NULL -- PaymentReference - text
)    
    """
    for tx in transactions:
        db_.execute(
            sql,
            invoice_id,
            tx.PerformingUserId,
            shift_id,
            tx.TxTime,
            tx.TxType,
            tx.TxFlag,
            tx.TxAmount,
            tx.NonCashAmount,
            tx.PaymentMethod,
        )


def reconcile_shift(shift_id: int, db_: Database):
    sql = """
SELECT
  SUM(tx.TxAmount) AS T 
FROM
  Finances.InvoiceTransactions AS tx
  INNER JOIN Finances.WorkShifts AS ws ON tx.WorkShiftId = ws.Id 
WHERE
  ws.Id = ? 
  AND tx.TxType = 10
    """
    total = db_.fetch_scalar(sql, "T", shift_id)
    if total is not None:
        sql = "UPDATE Finances.WorkShifts SET ReceiveAmount = ?, FinalBalance = ? WHERE Id = ?"
        db_.execute(sql, total, total, shift_id)


def insert_items(
    invoice_id: int, items: list[models.OrderedBillableItem], db_: Database
):
    sql = """
INSERT INTO PROE.OrderedBillableItems(
   InvoiceId
  ,BillableItemId
  ,UnitPrice
  ,Quantity
  ,DateCreated
  ,IsCancelled
) VALUES (
   ?   -- InvoiceId - bigint
  ,?   -- BillableItemId - smallint
  ,?   -- UnitPrice - smallmoney
  ,?   -- Quantity - smallint
  ,? -- DateCreated - smalldatetime
  ,0  -- IsCancelled - bit
)    
    """
    for item in items:
        db_.execute(
            sql,
            invoice_id,
            item.BillableItemId,
            item.UnitPrice,
            item.Quantity,
            item.DateCreated,
        )


def insert_tests(invoice_id: int, tests: list[models.OrderedLabTest], db_: Database):
    sql = """
INSERT INTO PROE.OrderedTests(
   InvoiceId
  ,LabTestId
  ,ResultBundleId
  ,IsCancelled
  ,UnitPrice
  ,WorkflowStage
  ,DateCreated
  ,LastModified
  ,ResultsETA
  ,LabNo
) VALUES (
   ?   -- InvoiceId - bigint
  ,?   -- LabTestId - smallint
  ,NULL -- ResultBundleId - bigint
  ,0  -- IsCancelled - bit
  ,?   -- UnitPrice - smallmoney
  ,?   -- WorkflowStage - tinyint
  ,? -- DateCreated - smalldatetime
  ,? -- LastModified - smalldatetime
  ,NULL -- ResultsETA - smalldatetime
  ,NULL -- LabNo - varchar(12)
)    
    """
    for t in tests:
        db_.execute(
            sql,
            invoice_id,
            t.LabTestId,
            t.UnitPrice,
            t.WorkflowStage,
            t.DateCreated,
            t.LastModified,
        )


def insert_bundles(invoice_id: int, bundles: list[models.ResultBundle], db_: Database):
    sql = """
INSERT INTO TestResults.ResultBundles(
   InvoiceId
  ,LabId
  ,ReportHeaderId
  ,IsActive
  ,TestResultType
  ,DisplayTitle
  ,ComponentLabTests
  ,DateCreated
  ,LastUpdated
  ,TATRank
  ,WorkflowStage
  ,FinalizingConsultantId
  ,FinalizingConsultantName
  ,CreatingUserId
  ,ResultNotes
) VALUES (
   ? -- InvoiceId - bigint
  ,?   -- LabId - smallint
  ,NULL -- ReportHeaderId - int
  ,1  -- IsActive - bit
  ,?   -- TestResultType - tinyint
  ,?  -- DisplayTitle - varchar(MAX)
  ,? -- ComponentLabTests - varchar(MAX)
  ,? -- DateCreated - smalldatetime
  ,? -- LastUpdated - smalldatetime
  ,?   -- TATRank - tinyint
  ,?   -- WorkflowStage - tinyint
  ,NULL -- FinalizingConsultantId - smallint
  ,NULL -- FinalizingConsultantName - varchar(160)
  ,NULL -- CreatingUserId - smallint
  ,NULL -- ResultNotes - varbinary(MAX)
)    
    """
    for b in bundles:
        db_.execute(
            sql,
            invoice_id,
            b.LabId,
            b.TestResultType,
            b.DisplayTitle,
            b.ComponentLabTests,
            b.DateCreated,
            b.LastUpdated,
            b.TATRank,
            b.WorkflowStage,
        )


def find_shift(user_id: int, dt: datetime.date, db_: Database) -> int | None:
    sql = "SELECT * FROM Finances.[WorkShifts] WHERE CAST(StartTime AS DATE) = ? AND UserId = ?"
    row = db_.fetch(sql, dt, user_id)
    if row:
        return int(row["Id"])
    return None


def create_shift(user_id: int, dt: datetime.date, db_: Database) -> int:
    sql = """
INSERT INTO Finances.WorkShifts(
   UserId
  ,IsClosed
  ,StartTime
  ,EndTime
  ,LastUpdated
  ,NumOrders
  ,AdditionalBalance
  ,ReceiveAmount
  ,DiscountAmount
  ,DiscountRebateAmount
  ,RefundAmount
  ,FinalBalance
  ,UserNotes
  ,NonCashAmount
) VALUES (
   ?   -- UserId - smallint
  ,0  -- IsClosed - bit
  ,? -- StartTime - smalldatetime
  ,NULL -- EndTime - smalldatetime
  ,getdate() -- LastUpdated - smalldatetime
  ,0   -- NumOrders - smallint
  ,0   -- AdditionalBalance - money
  ,0   -- ReceiveAmount - money
  ,0   -- DiscountAmount - money
  ,0   -- DiscountRebateAmount - money
  ,0   -- RefundAmount - money
  ,0   -- FinalBalance - money
  ,NULL -- UserNotes - varchar(MAX)
  ,0   -- NonCashAmount - money
)    
    """
    with db_.cursor() as cur:
        cur.execute(sql, user_id, dt)
        cur.execute("SELECT @@IDENTITY AS ID;")
        return cur.fetchone()[0]

    return None
