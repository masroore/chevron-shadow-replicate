import datetime

import yaml
from src import db, utils, models, catalogs
import arrow

with open("config.yml", "r") as file:
    config = yaml.safe_load(file)

DB_SRC = config["db"]["src"]
DB_DEST = config["db"]["dst"]


def fetch_invoices(dt: datetime.date) -> list[models.LabOrder]:
    with db.Database.make(DB_SRC) as src_db:
        sql = """
SELECT
    ord.*,
    COALESCE(ref.FullName, ord.ReferrerCustomName, '') AS ReferrerCustomName
FROM
    PROE.PatientLabOrders AS ord
    LEFT JOIN [Catalog].Referrers AS ref ON ord.ReferrerId = ref.Id 
WHERE
    CAST (ord.OrderDateTime AS DATE) = ?        
        """
        rows = src_db.fetch_all(sql, dt)
        return [models.LabOrder(**row) for row in rows]


test_cat = catalogs.get_test_catalog(db.Database.make(DB_SRC))
for t in test_cat.keys():
    utils.croak(test_cat.find(t).model_dump())
for u in catalogs.get_staff_catalog(db.Database.make(DB_SRC)):
    utils.croak(u.model_dump())

invoices = fetch_invoices(arrow.now().date())
# for inv in invoices: utils.croak(inv.model_dump())
