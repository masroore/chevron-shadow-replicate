from datetime import date

import arrow
import yaml

from src import db, dal
from src.utils import croak
from src.catalogs import OrderContext

with open("config.yml", "r") as file:
    config = yaml.safe_load(file)

DB_SRC = config["db"]["src"]
DB_DEST = config["db"]["dst"]


def dest_last_invoice_id(dt: date) -> int | None:
    with db.Database.make(DB_DEST) as db_:
        sql = "SELECT MAX(SourceInvoiceId) AS _id_ FROM PROE.PatientLabOrders WHERE CAST (OrderDateTime AS DATE) = ?"
        return db_.fetch_scalar(sql, "_id_", dt)


def src_scan_orders(dt: date, last_id: int | None) -> list[OrderContext]:
    with db.Database.make(DB_SRC) as db_src:
        test_cat = dal.get_test_catalog(db_src)
        # staff = dal.get_staff_catalog(db_src)
        if last_id is not None and last_id > 0:
            invoices = dal.fetch_invoices_after(dt, last_id, db_src)
        else:
            invoices = dal.fetch_invoices(dt, db_src)
        croak(f"Found {len(invoices)} invoices for {dt}, Last: {last_id}")
        contexts = []
        for inv in invoices:
            ctx = OrderContext(inv)
            ctx.scan(db_src)
            ctx.sanitize_tests(test_cat)
            contexts.append(ctx)
        return contexts


if __name__ == "__main__":
    dt = arrow.now().date()
    last_id = dest_last_invoice_id(dt)
    orders = src_scan_orders(dt, last_id)
