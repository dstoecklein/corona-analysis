from database.db_helper import Database

DB = Database()


pk = DB.get_unique_cols("_calendar_days")
print(pk)