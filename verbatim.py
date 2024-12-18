import yaml
from src import db

with open("config.yml", "r") as file:
    config = yaml.safe_load(file)

DB_SRC = config["db"]["src"]
DB_DEST = config["db"]["dst"]

with db.Database.make(DB_SRC) as src_db:
    src_db.connect()
    row = src_db.fetch_scalar("SELECT COUNT(*) AS X FROM Staff.Users", "X")
    print(row)
