import arrow
import yaml

from src import db, dal
from src.utils import croak
from src.catalogs import OrderContext

with open("config.yml", "r") as file:
    config = yaml.safe_load(file)

DB_SRC = config["db"]["src"]
DB_DEST = config["db"]["dst"]


def scan_source_orders() -> list[OrderContext]:
    with db.Database.make(DB_SRC) as db_src:
        test_cat = dal.get_test_catalog(db_src)
        # staff = dal.get_staff_catalog(db_src)

        dt = arrow.now().date()
        invoices = dal.fetch_invoices(dt, db_src)
        croak(f"Found {len(invoices)} invoices for {dt}")
        contexts = []
        for inv in invoices:
            ctx = OrderContext(inv)
            ctx.scan(db_src)
            ctx.sanitize_tests(test_cat)
            contexts.append(ctx)
        return contexts


if __name__ == "__main__":
    orders = scan_source_orders()
