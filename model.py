import sqlite3
from sqlite3 import Error
import requests
import xml.etree.ElementTree as ET
#in case if we need error report---for now I better keep with out using this function but good stuff
def create_connection(db_file):
   
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn
def runModelPy():
    conn = sqlite3.connect('rarr.db')

    db = conn.cursor()
    sql1 = 'DELETE FROM sensorData'
    #sql2 = 'DELETE FROM sensorStatusData'
    #sql3 = 'DELETE FROM  CurrentsensorData'
    db.execute(sql1)
    conn.commit()
    #db.execute(sql2)
    #conn.commit()
    #db.execute(sql3)
    #conn.commit()
    #db.execute("create Table sensorData(site_id text, sensor_id text, data_time text, stage(ft) real)")
    db.execute("create Table sensorDatum(Site_Name text, site_id text, Datum_ft real)")
    db.execute("create Table sensorStatusData(site_id text, sensor_id text, normal integer, active integer, valid integer)")
    db.execute("create Table CurrentsensorData(site_id text, sensor_id text, data_time text, stage_ft real)")
    url = 'https://meckgisdev.mecklenburgcountync.gov/api/contrail?method=GetSensorData&system=c57f3913-ac01-4aa7-b633-e8311f45f74a&tz=ET&class=20'
    url_meta = 'https://meckgisdev.mecklenburgcountync.gov/api/contrail?method=GetSensormetaData&system=c57f3913-ac01-4aa7-b633-e8311f45f74a&tz=ET&class=20&active=1&normal=1&valid=1'
    res = requests.get(url)
    res_meta = requests.get(url_meta)
    sensorDatas = res.text
    sensorMeta = res_meta.text
    row = ET.fromstring(sensorDatas)
    rowMeta = ET.fromstring(sensorMeta)
    sql = ''' INSERT INTO sensorData(site_id, sensor_id, data_time, stage_ft)
                VALUES(?,?,?,?) '''
    sql_status = ''' INSERT INTO sensorStatusData(site_id, sensor_id, normal, active, valid)
                VALUES(?,?,?,?,?) '''
    sql_current = ''' INSERT INTO CurrentsensorData(site_id, sensor_id, data_time, stage_ft)
                VALUES(?,?,?,?) '''          
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
            stage = x.find('data_value').text
        else:
            stage = None
        #text = x.find('data_time')
        data = (site_id, sensor_id, data_time, stage)
        db.execute(sql, data)
        conn.commit()
runModelPy()       
#db.execute("select * from sensorData")

#conn.commit()

#conn.close()
