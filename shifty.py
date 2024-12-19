from datetime import date

import yaml
import arrow

from src import db, dal
from src.db import Database
from src.models import LabOrder
from src.utils import croak

with open("config.yml", "r") as file:
    config = yaml.safe_load(file)

DB_DEST = config["db"]["dst"]


def ensure_shift(orders: list[LabOrder], dt: date, db_: Database):
    shifts = []
    for ord in orders:
        croak(f"INSERT #{ord.InvoiceId} - {ord.OrderId}")
        shift_id = dal.find_shift(ord.OrderingUserId, dt, db_)
        if not shift_id:
            shift_id = dal.create_shift(ord.OrderingUserId, dt, db_)

        if shift_id:
            db_.execute(
                "UPDATE PROE.PatientLabOrders SET WorkShiftId = ? WHERE InvoiceId = ?",
                shift_id,
                ord.InvoiceId,
            )
            db_.execute(
                "UPDATE Finances.InvoiceTransactions SET WorkShiftId = ? WHERE InvoiceId = ?",
                shift_id,
                ord.InvoiceId,
            )
            shifts.append(shift_id)

    for sid in sorted(set(shifts)):
        dal.reconcile_shift(sid, db_)


with db.Database.make(DB_DEST) as db_:
    start_date = arrow.get("2024-12-11")
    end_date = arrow.get("2024-12-18")

    dates = db_.fetch_scalars(
        "SELECT DISTINCT CAST(OrderDateTime AS DATE) AS D FROM PROE.PatientLabOrders",
        "D",
    )
    dates = sorted(set(dates))
    croak(f"Found {len(dates)} unique dates")

    dt = start_date
    while dt <= end_date:
        invoices = dal.fetch_invoices(dt.date(), db_)
        croak(f"Found {len(invoices)} invoices for {dt.date()}")
        ensure_shift(invoices, dt.date(), db_)
        dt = dt.shift(days=1)
