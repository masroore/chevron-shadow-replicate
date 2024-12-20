import time
from datetime import date

import arrow
import yaml

from src import db, dal, utils
from src.utils import croak
from src.catalogs import OrderContext

config = utils.get_config()
DB_SRC = config["db"]["src"]
DB_DEST = config["db"]["dst"]
WAIT_SECONDS = int(config["main"]["wait_seconds"])


def dest_last_invoice_id(dt: date) -> int | None:
    with db.Database.make(DB_DEST) as db_:
        return dal.last_src_invoice_id(dt, db_)


def scan_insert_orders(dt: date, last_id: int | None) -> list[OrderContext]:
    with db.Database.make(DB_SRC) as db_:
        shift_ids = []
        test_cat = dal.get_test_catalog(db_)
        # staff = dal.get_staff_catalog(db_src)
        if last_id is not None and last_id > 0:
            invoices = dal.fetch_invoices_after(dt, last_id, db_)
        else:
            invoices = dal.fetch_invoices(dt, db_)
        croak(f"Found {len(invoices)} invoices for {dt}, Last: {last_id}")
        contexts = []
        for inv in invoices:
            croak(f"Scanning #{inv.InvoiceId}")
            ctx = OrderContext(inv)
            ctx.scan(db_)
            # ctx.sanitize_tests(test_cat)
            contexts.append(ctx)
            shift_id = dest_insert_chain(ctx)
            if shift_id:
                shift_ids.append(shift_id)

        for shift_id in sorted(set(shift_ids)):
            dal.reconcile_shift(shift_id, False, db_)

        return contexts


def dest_insert_chain(order: OrderContext) -> int | None:
    croak(f"INSERT #{order.order.InvoiceId} - {order.order.OrderId}")
    with db.Database.make(DB_DEST) as db_:
        shift_id = dal.find_shift(order.order.OrderingUserId, arrow.now().date(), db_)
        if not shift_id:
            shift_id = dal.create_shift(
                order.order.OrderingUserId, arrow.now().date(), db_
            )

        order.order.WorkShiftId = shift_id
        if not dal.insert_order(order.order, db_):
            return None

        invoice_id = dal.shadow_id_for_source_id(order.order.InvoiceId, db_)
        croak(f"Src: {order.order.InvoiceId} -> Dest: {invoice_id}")
        dal.insert_master(invoice_id, order.master, db_)
        dal.insert_primal(invoice_id, order.primal, db_)
        dal.insert_transactions(invoice_id, order.transactions, shift_id, db_)
        dal.insert_items(invoice_id, order.items, db_)
        dal.insert_tests(invoice_id, order.tests, db_)
        dal.insert_bundles(invoice_id, order.bundles, db_)
        db_.commit()
        return shift_id


def reconcile(dt: date):
    with db.Database.make(DB_DEST) as db_:
        shift_ids = dal.get_shifts(dt, db_)
        for sid in shift_ids:
            dal.reconcile_shift(sid, True, db_)


def looper(wait: int):
    while True:
        dt = arrow.now().date()
        last_id = dest_last_invoice_id(dt)
        scan_insert_orders(dt, last_id)
        croak(f"Zzzzzz {wait}s...")
        time.sleep(wait)


if __name__ == "__main__":
    reconcile(arrow.now().date())
    looper(WAIT_SECONDS)
