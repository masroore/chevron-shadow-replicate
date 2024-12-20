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

DB_SRC = config["db"]["src"]
DB_DEST = config["db"]["dst"]
BARRIER = config["barrier"]["daily"]
BARRIER_JITTER = config["barrier"]["jitter"]


def get_rand_hwm() -> int:
    return int(BARRIER + random.randint(-BARRIER_JITTER, BARRIER_JITTER))


def purge_orders(dt: date):
    with db.Database.make(DB_DEST) as db_:
        invoices = dal.fetch_invoices(dt, db_)
        croak(f"Found {len(invoices)} invoices for {dt}")

        for ord in invoices:
            croak(f"Purging #{ord.InvoiceId} - {ord.OrderId}")
            dal.purge_order_chain(ord.InvoiceId, db_)


def recreate_shifts(orders: list[OrderContext], dt: date, db_: Database) -> list[int]:
    user_ids = set([x.order.OrderingUserId for x in orders])
    shift_map = {}

    for uid in user_ids:
        shift_id = dal.find_shift(uid, dt, db_)
        if not shift_id:
            shift_id = dal.create_shift(uid, dt, db_)
        if shift_id:
            shift_map[uid] = shift_id

    shifts: list[int] = []
    for ord in orders:
        shift_id = shift_map.get(ord.order.OrderingUserId)

        if shift_id:
            db_.execute(
                "UPDATE PROE.PatientLabOrders SET WorkShiftId = ? WHERE InvoiceId = ?",
                shift_id,
                ord.order.InvoiceId,
            )
            db_.execute(
                "UPDATE Finances.InvoiceTransactions SET WorkShiftId = ? WHERE InvoiceId = ?",
                shift_id,
                ord.order.InvoiceId,
            )
            shifts.append(shift_id)

    return sorted(set(shifts))


def src_scan_orders(dt: date, max_net: int) -> list[OrderContext]:
    net_total = 0
    with db.Database.make(DB_SRC) as db_:
        test_cat = dal.get_test_catalog(db_)
        invoices = dal.fetch_invoices(dt, db_)
        croak(f"Found {len(invoices)} invoices for {dt} | HWM: {max_net:,.0f}")

        contexts = []
        for i, inv in enumerate(invoices):
            croak(
                f"{i:04d} Scanning #{inv.InvoiceId} {inv.OrderId} | Net: {net_total:,.0f}"
            )
            ctx = OrderContext(inv)
            ctx.scan(db_)
            net_total += ctx.master.NetPayable
            if net_total >= max_net:
                croak(
                    f"Filtered {len(contexts)} orders | HWM: {max_net} | Actual: {net_total}"
                )
                break

            ctx.sanitize_tests(test_cat)
            contexts.append(ctx)

    croak(f"Time-spreading {len(contexts)} orders")
    time_spread_invoices(contexts, dt)

    return contexts


def dest_insert_chain(order: OrderContext, db_: Database):
    croak(f"INSERT #{order.order.InvoiceId} - {order.order.OrderId}")
    if not dal.insert_order(order.order, db_):
        return

    invoice_id = dal.shadow_id_for_source_id(order.order.InvoiceId, db_)
    croak(f"Src: {order.order.InvoiceId} -> Dest: {invoice_id}")
    dal.insert_master(invoice_id, order.master, db_)
    dal.insert_primal(invoice_id, order.primal, db_)
    dal.insert_transactions(invoice_id, order.transactions, db_)
    dal.insert_items(invoice_id, order.items, db_)
    dal.insert_tests(invoice_id, order.tests, db_)
    dal.insert_bundles(invoice_id, order.bundles, db_)
    db_.commit()


def filter_orders(orders: list[OrderContext], barrier: int) -> list[OrderContext]:
    result = []
    total = 0
    for o in orders:
        total += o.master.NetPayable
        if total > barrier:
            break
        result.append(o)
    croak(f"Filtered {len(result)} orders | HWM: {barrier} | Actual: {total}")
    return result


def reconcile(shifts: list[int]):
    croak(f"Found {len(shifts)} shifts")
    with db.Database.make(DB_DEST) as db_:
        for sid in sorted(set(shifts)):
            croak(f"Reconciling shift #{sid}")
            dal.reconcile_shift(sid, True, db_)


def time_spread_invoices(orders: list[OrderContext], dt: date):
    sod = datetime.combine(dt, datetime.min.time()) + timedelta(
        hours=8, minutes=random.randint(3, 12)
    )
    eod = datetime.combine(dt, datetime.min.time()) + timedelta(
        hours=21, minutes=random.randint(45, 57)
    )
    total_seconds = (eod - sod).total_seconds()
    interval = total_seconds / len(orders)

    for i, ord in enumerate(orders):
        ord.order.OrderDateTime = sod + timedelta(seconds=i * interval)


def populate_shadow(orders: list[OrderContext], dt: date, barrier: int):
    with db.Database.make(DB_DEST) as db_:
        recreate_shifts(orders, dt, db_)
        for ord in orders:
            dest_insert_chain(ord)


if __name__ == "__main__":
    dt = arrow.now().date()
    purge_orders(dt)
    barrier = get_rand_hwm()
    orders = src_scan_orders(dt, barrier)
    # orders = filter_orders(orders, barrier)
    populate_shadow(orders, dt, barrier)
