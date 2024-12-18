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
        return dal.last_src_invoice_id(dt, db_)


def src_scan_orders(dt: date, last_id: int | None) -> list[OrderContext]:
    with db.Database.make(DB_SRC) as db_:
        test_cat = dal.get_test_catalog(db_)
        # staff = dal.get_staff_catalog(db_src)
        if last_id is not None and last_id > 0:
            invoices = dal.fetch_invoices_after(dt, last_id, db_)
        else:
            invoices = dal.fetch_invoices(dt, db_)
        croak(f"Found {len(invoices)} invoices for {dt}, Last: {last_id}")
        contexts = []
        for inv in invoices:
            ctx = OrderContext(inv)
            ctx.scan(db_)
            ctx.sanitize_tests(test_cat)
            contexts.append(ctx)
        return contexts


def dest_insert_chain(order: OrderContext):
    croak(f"INSERT #{order.order.InvoiceId} - {order.order.OrderId}")
    with db.Database.make(DB_DEST) as db_:
        if not dal.insert_order(order.order, db_):
            return
        


if __name__ == "__main__":
    dt = arrow.now().date()
    last_id = dest_last_invoice_id(dt)
    orders = src_scan_orders(dt, last_id)
    for o in orders:
        dest_insert_chain(o)
