from datetime import date

import arrow
import yaml

from src import db, dal, models
from src.utils import croak
from src.catalogs import OrderContext

with open("config.yml", "r") as file:
    config = yaml.safe_load(file)

DB_SRC = config["db"]["src"]
DB_DEST = config["db"]["dst"]


def purge_orders(dt: date):
    with db.Database.make(DB_DEST) as db_:
        invoices = dal.fetch_invoices(dt, db_)
        croak(f"Found {len(invoices)} invoices for {dt}")

        for ord in invoices:
            dal.purge_order_chain(ord.InvoiceId, db_)


if __name__ == "__main__":
    purge_orders(arrow.now().date())
