import sqlite3

conn = sqlite3.connect('rarr.db')

db = conn.cursor()

#db.execute("create Table sensorData(site_id text, sensor_id text, data_time text, data_valu real)")

db.execute("select * from sensorData")

conn.commit()

conn.close()
