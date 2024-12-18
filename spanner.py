import random
from datetime import date, datetime, timedelta
import arrow
import yaml

from src import db, dal
from src.db import Database
from src.models import LabOrder
from src.utils import croak
from src.catalogs import OrderContext

with open("config.yml", "r") as file:
    config = yaml.safe_load(file)

DB_DEST = config["db"]["dst"]


def time_spread_invoices(orders: list[LabOrder], dt: date, db_: Database):
    sod = datetime.combine(dt, datetime.min.time()) + timedelta(hours=8)
    eod = datetime.combine(dt, datetime.min.time()) + timedelta(hours=22)
    total_seconds = (eod - sod).total_seconds()
    interval = total_seconds / len(orders)

    for i, ord in enumerate(orders):
        order_time = sod + timedelta(seconds=i * interval)
        croak(
            f"#{ord.InvoiceId} ({ord.OrderId}) Before: {ord.OrderDateTime} After: {order_time}"
        )
        db_.execute(
            "UPDATE PROE.PatientLabOrders SET OrderDateTime = ? WHERE InvoiceId = ?",
            order_time,
            ord.InvoiceId,
        )


with db.Database.make(DB_DEST) as db_:
    dates = db_.fetch_scalars(
        "SELECT DISTINCT CAST(OrderDateTime AS DATE) AS D FROM PROE.PatientLabOrders",
        "D",
    )
    dates = sorted(set(dates))
    croak(f"Found {len(dates)} unique dates")
    for dt in dates:
        invoices = dal.fetch_invoices(dt, db_)
        croak(f"Found {len(invoices)} invoices for {dt}")
        time_spread_invoices(invoices, dt)
