import sqlite3

con = sqlite3.connect("testDb.db")
cur = con.cursor()
cur.execute("CREATE TABLE test(titel1, titel2, titel3)")

print("TypeScriptGeneratedFilesManager")
