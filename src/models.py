import enum
from datetime import datetime, date

from pydantic import BaseModel


class LabTest(BaseModel):
    Id: int
    TestSKU: str
    ShortName: str
    CanonicalName: str
    ListPrice: int
    LabName: str


class Invoice(BaseModel):
    InvoiceId: int
    PaymentStatus: int = 0
    GrossPayable: int = 0
    DiscountAmount: int = 0
    TaxAmount: int = 0
    SurchargeAmount: int = 0
    NetPayable: int = 0
    PaidAmount: int = 0
    DueAmount: int = 0
    RefundAmount: int = 0
    PaidUpReferral: int = 0
    DateCreated: datetime


class TransactionType(enum.IntEnum):
    Unknown = 0
    Payment = 10
    Refund = 20
    CashDiscount = 30
    DiscountRebate = 40


class InvoiceTransaction(BaseModel):
    Id: int
    InvoiceId: int
    WorkShiftId: int | None = None
    PerformingUserId: int | None = None
    TxTime: datetime
    TxType: TransactionType = TransactionType.Unknown
    TxFlag: int
    TxAmount: int
    NonCashAmount: int
    PaymentMethod: int


class OrderedLabTest(BaseModel):
    Id: int
    InvoiceId: int
    LabTestId: int
    LabId: int
    ResultBundleId: int | None = None
    TestName: str | None = None
    UnitPrice: int
    WorkflowStage: int
    DateCreated: datetime
    LastModified: datetime | None = None


class ResultBundle(BaseModel):
    Id: int
    InvoiceId: int | None = None
    LabId: int
    ReportHeaderId: int | None = None
    TestResultType: int
    TATRank: int
    WorkflowStage: int
    DisplayTitle: str | None = None
    ComponentLabTests: str | None = None
    DateCreated: datetime
    LastUpdated: datetime | None = None


class OrderedBillableItem(BaseModel):
    Id: int
    InvoiceId: int
    BillableItemId: int | None = None
    BillableItemName: str | None = None
    UnitPrice: int
    Quantity: int
    DateCreated: datetime
    LastModified: datetime | None = None


class User(BaseModel):
    Id: int
    UserName: str
    DisplayName: str


class LabOrder(BaseModel):
    InvoiceId: int
    OrderDateTime: datetime
    LastModified: datetime
    OrderId: str
    WorkflowStage: int
    ReferrerId: int | None = None
    OrderingUserId: int | None = None
    WorkShiftId: int | None = None
    DisallowReferral: int | None = None
    Title: str | None = None
    FirstName: str
    LastName: str | None = None
    DoB: date | None = None
    Sex: int
    Age: str | None = None
    PhoneNumber: str | None = None
    IsCancelled: bool
    IsReferrerUnknown: bool
    ReferrerCustomName: str | None = None
    OrderNotes: str | None = None
    WebAccessToken: str | None = None
    IsExternalSubOrder: bool = False
    SourceInvoiceId: int | None = None
