import sqlite3
import requests
import xml.etree.ElementTree as ET

conn = sqlite3.connect('rarr.db')

db = conn.cursor()

#db.execute("create Table sensorData(site_id text, sensor_id text, data_time text, data_valu real)")
url = 'https://meckgisdev.mecklenburgcountync.gov/api/contrail?method=GetSensorData&system=c57f3913-ac01-4aa7-b633-e8311f45f74a&class=20'

res = requests.get(url)
sensorDatas = res.text
row = ET.fromstring(sensorDatas)
for x in row[0][0].findall('row'):
    if x.find('site_id').text:
        site_id = x.find('site_id').text
    else:
        site_id = None
    if x.find('sensor_id').text:
        sensor_id = x.find('sensor_id').text
    else:
        sensor_id = None
    if x.find('data_time') is None:
        data_time = None
    else:
        data_time = x.find('data_time').text
    if x.find('data_value').tag:
        data_value = x.find('data_value').text
    else:
        data_value = None
    #text = x.find('data_time')
    print(site_id,sensor_id, data_time, data_value )

#db.execute("select * from sensorData")

#conn.commit()

#conn.close()
