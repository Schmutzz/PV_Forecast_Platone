import Sqlite
import Sandbox


import sqlite3
import pandas as pd


DB_Name = "PV_Platone.db"
conn = sqlite3.connect(DB_Name)
cur = conn.cursor()
cur.execute("CREATE TABLE PV_HEISENBERG_71 (Timestamp DATE, Power_DC1 REAL)")

df = pd.read_csv("rawdata/PV Generation-data-2022-11-01 12_52_19.csv", delimiter=",", header=0)
print(df)
df.to_sql("PV_HEISENBERG_71", conn, if_exists='append', index=False)



conn.close()

