import sys
from datetime import datetime
from typing import Any

from prettyprinter import cpprint
import yaml


def dump(*args, **kwargs):
    for arg in args:
        if type(arg) == str:
            print(arg)
        else:
            width = 120
            cpprint(arg, width=width, ribbon_width=width, **kwargs)


def dd(*args, **kwargs):
    dump(*args, **kwargs)
    sys.exit()


def croak(msg: str):
    fmt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{fmt}] {msg}")


def get_config() -> dict:
    with open("config.yml", "r") as file:
        return yaml.safe_load(file)
