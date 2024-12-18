from src import models, db


class Catalog:
    def __init__(self, items: list):
        self._items = items

    def find(self, id_: int):
        for item in self._items:
            if item.Id == id_:
                return item
        return None

    def keys(self) -> list[int]:
        return [item.Id for item in self._items]

    def __iter__(self):
        return iter(self._items)


def get_test_catalog(conn: db.Database) -> Catalog:
    sql = """
SELECT
  LabTests.*,
  Labs.Name AS LabName 
FROM
  [Catalog].LabTests
  INNER JOIN [Catalog].Labs ON LabTests.PerformingLabId = Labs.Id 
WHERE
  LabTests.IsActive = 1 
  AND Labs.IsActive = 1    
    """
    rows = conn.fetch_all(sql)
    return Catalog([models.LabTest(**row) for row in rows])


def get_staff_catalog(conn: db.Database) -> Catalog:
    sql = """
SELECT
  *
FROM
  Staff.Users
WHERE
  IsActive = 1 
    """
    rows = conn.fetch_all(sql)
    return Catalog([models.User(**row) for row in rows])
