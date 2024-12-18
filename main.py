import yaml
from src import db


def create_dsn(config: dict[str, str]) -> str:
    return ";".join(f"{k}={v}" for k, v in config.items())


with open("config.yml", "r") as file:
    config = yaml.safe_load(file)

source_dsn = create_dsn(config["db"]["src"])
dest_dsn = create_dsn(config["db"]["dst"])

with db.Database.make(config["db"]["src"]) as src_db:
    src_db.connect()
    row = src_db.fetch_val("SELECT COUNT(*) FROM Staff.Users")
    print(row)
