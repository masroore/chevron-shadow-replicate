from datetime import datetime, date

from pydantic import BaseModel


class LabTest(BaseModel):
    Id: int
    TestSKU: str
    ShortName: str
    CanonicalName: str
    ListPrice: int
    LabName: str


class OrderedLabTest(BaseModel):
    Id: int
    InvoiceId: int
    LabTestId: int
    ResultBundleId: int | None = None
    TestName: str | None = None
    UnitPrice: int
    WorkflowStage: int
    DateCreated: datetime
    LastModified: datetime | None = None


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
