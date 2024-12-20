import random
from datetime import date, datetime, timedelta

import arrow

from src import db, dal, utils
from src.catalogs import OrderContext
from src.db import Database
from src.utils import croak

config = utils.get_config()
DB_SRC = config["db"]["src"]
DB_DEST = config["db"]["dst"]
BARRIER = int(config["barrier"]["daily"])
BARRIER_JITTER = int(config["barrier"]["jitter"])
START_HOURS = int(config["main"]["business_hours"]["start"])
END_HOURS = int(config["main"]["business_hours"]["end"])


def get_rand_hwm() -> int:
    return int(BARRIER + random.randint(-BARRIER_JITTER, BARRIER_JITTER))


def purge_orders(dt: date):
    with db.Database.make(DB_DEST) as db_:
        invoices = dal.fetch_invoices(dt, db_)
        croak(f"Found {len(invoices)} invoices for {dt}")

        for ord in invoices:
            croak(f"Purging #{ord.InvoiceId} - {ord.OrderId}")
            dal.purge_order_chain(ord.InvoiceId, db_)

        croak(f"Purging work shifts")
        dal.purge_work_shifts(dt, db_)


def recreate_shifts(
    orders: list[OrderContext], dt: date, db_: Database
) -> dict[int, int]:
    user_ids = set([x.order.OrderingUserId for x in orders])
    shift_map = {}

    for uid in user_ids:
        shift_id = dal.find_shift(uid, dt, db_)
        if not shift_id:
            shift_id = dal.create_shift(uid, dt, db_)
        if shift_id:
            shift_map[uid] = shift_id

    """
    for ord in orders:
        shift_id = shift_map.get(ord.order.OrderingUserId)

        if shift_id:
            db_.execute(
                "UPDATE PROE.PatientLabOrders SET WorkShiftId = ? WHERE OrderingUserId = ? AND CAST(OrderDateTime AS DATE) = ?",
                shift_id,
                ord.order.OrderingUserId,
                ord.order.OrderDateTime.date(),
            )
            db_.execute(
                "UPDATE Finances.InvoiceTransactions SET WorkShiftId = ? WHERE InvoiceId = ?",
                shift_id,
                ord.order.InvoiceId,
            )
    """

    return shift_map


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


def insert_lab_order_chain(ctx: OrderContext, shift_map: dict[int, int], db_: Database):
    croak(f"INSERT #{ctx.order.InvoiceId} - {ctx.order.OrderId}")
    shift_id = shift_map.get(ctx.order.OrderingUserId)
    ctx.order.WorkShiftId = shift_id
    if not dal.insert_order(ctx.order, db_):
        return

    invoice_id = dal.shadow_id_for_source_id(ctx.order.InvoiceId, db_)
    croak(f"Src: {ctx.order.InvoiceId} -> Dest: {invoice_id}")
    dal.insert_master(invoice_id, ctx.master, db_)
    dal.insert_primal(invoice_id, ctx.primal, db_)
    dal.insert_transactions(invoice_id, ctx.transactions, shift_id, db_)
    dal.insert_items(invoice_id, ctx.items, db_)
    dal.insert_tests(invoice_id, ctx.tests, db_)
    dal.insert_bundles(invoice_id, ctx.bundles, db_)
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


def reconcile_shifts(shift_map: dict[int, int], db_: Database):
    croak(f"Found {len(shift_map.keys())} shifts")
    for uid in shift_map.keys():
        croak(f"Reconciling shift #{shift_map[uid]}")
        dal.reconcile_shift(shift_map[uid], True, db_)


def time_spread_invoices(orders: list[OrderContext], dt: date):
    sod = datetime.combine(dt, datetime.min.time()) + timedelta(
        hours=START_HOURS, minutes=random.randint(3, 12)
    )
    eod = datetime.combine(dt, datetime.min.time()) + timedelta(
        hours=END_HOURS - 1, minutes=random.randint(45, 57)
    )
    total_seconds = (eod - sod).total_seconds()
    interval = total_seconds / len(orders)

    for i, ord in enumerate(orders):
        ord.order.OrderDateTime = sod + timedelta(seconds=i * interval)


def populate_shadow(orders: list[OrderContext], dt: date):
    with db.Database.make(DB_DEST) as db_:
        shift_map = recreate_shifts(orders, dt, db_)
        for ord in orders:
            insert_lab_order_chain(ord, shift_map, db_)

        reconcile_shifts(shift_map, db_)


if __name__ == "__main__":
    start_date = arrow.get("2024-12-11")
    end_date = arrow.get("2024-12-19")
    dt = start_date

    while dt <= end_date:
        purge_orders(dt)
        barrier = get_rand_hwm()
        orders = src_scan_orders(dt, barrier)
        populate_shadow(orders, dt)
        dt = dt.shift(days=1)
