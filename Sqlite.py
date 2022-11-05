import sqlite3
import csv
import pandas as pd

"""con = sqlite3.connect("PV_Platone.db")
cur = con.cursor()
#cur.execute("CREATE TABLE test(titel1, titel2, titel3)")
#cur.execute("CREATE TABLE PV_HEISENBERG_71 (Timestamp DATE, Power_DC1 REAL)")
con.close()"""


class database_Controller(object):

    def __init__(self, database):
        self.database = database

    def openDB(self, name):
        con = sqlite3.connect(name)
        cur = con.cursor()
        return cur

    def import_data_csv(self, csv_file, table_name, conn):

        df = pd.read_csv(csv_file)
        df.to_sql(table_name, conn, if_exists='append', index=False)



