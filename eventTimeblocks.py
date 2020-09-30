import sqlite3
from sqlite3 import Error
import requests
import xml.etree.ElementTree as ET
import time
from datetime import date, timedelta, datetime
import sys
import decimal

start_time = time.time()
run_datetime = datetime.now()

# def create_connection(db_file):
   
#     conn = None
#     try:
#         conn = sqlite3.connect(db_file)
#     except Error as e:
#         print(e)

#     return conn

conn = sqlite3.connect('rarr.db')
cur = conn.cursor()

cur.execute("PRAGMA synchronous = OFF")
cur.execute("PRAGMA journal_mode = OFF")

logMessage = "successful"

def updateLogMessage(message):
    global logMessage
    logMessage = message

def logEventOutcome():
    print(logMessage)


def getEventData(startDate, endDate, eventName):

    rawDatabase = eventName + "Raw"
    maxDatabase = eventName +"Max"

    sqlDropRaw = "DROP TABLE IF EXISTS " + rawDatabase
    sqlCreateRaw = "CREATE TABLE IF NOT EXISTS " + rawDatabase + " (site_id TEXT, sensor_id TEXT, data_time TEXT, data_value TEXT)"

    sqlDrop = "DROP TABLE IF EXISTS " + eventName
    sqlCreate = "CREATE TABLE IF NOT EXISTS " + eventName + " (site_id TEXT, sensor_id TEXT, data_time TEXT, data_value TEXT)"

    sqlDropMax = "DROP TABLE IF EXISTS " + maxDatabase
    sqlCreateMax = "CREATE TABLE IF NOT EXISTS " + maxDatabase + " (site_id TEXT, sensor_id TEXT, data_time TEXT, data_value TEXT)"

    sqlCreateEvents = "CREATE TABLE IF NOT EXISTS Events (event_id INTEGER PRIMARY KEY, event_name TEXT UNIQUE, start_date TEXT, end_date TEXT, total_readings TEXT, gages_read TEXT, readings_table TEXT, max_table TEXT, createdDate TEXT, lastUpdated TEXT)"

    sqlCreateEventLogs = "CREATE TABLE IF NOT EXISTS eventLogs (attempt_id INTEGER PRIMARY KEY, event_name TEXT,  start_date TEXT, end_date TEXT, attempt_dateTime TEXT, log_Message TEXT)"

    try:
        cur.execute(sqlDropRaw)
        cur.execute(sqlCreateRaw)
        cur.execute(sqlDrop)
        cur.execute(sqlCreate)
        cur.execute(sqlDropMax)
        cur.execute(sqlCreateMax)
        cur.execute(sqlCreateEvents)
        cur.execute(sqlCreateEventLogs)

    except sqlite3.Error as error:
        updateLogMessage("Error creating data tables: " + str(error))

    #downloadGageDataTimeblocked(eventName, startDate, endDate, rawDatabase, eventName, maxDatabase)


# @profile
def downloadGageDataTimeblocked(eventName, startDate, endDate, rawDataTable, finalDataTable, maxDataTable):

    errorMessage = "Unknown error writing gage data to table."

    start_date = date(int(startDate[0]), int(startDate[1]), int(startDate[2]))
    end_date = date(int(endDate[0]), int(endDate[1]), int(endDate[2]))
    delta = end_date - start_date
    days = delta.days

    sqlInsert = "INSERT INTO " + rawDataTable + " (site_id, sensor_id, data_time, data_value) VALUES(?,?,?,?)"
    sqlFinalJoin = "INSERT INTO " + finalDataTable + " SELECT site_id, sensor_id, data_time, data_value FROM (SELECT * FROM " + rawDataTable + " JOIN sensorDatum ON " + rawDataTable + ".site_id = sensorDatum.site_id)"
    sqlMaxJoin = """WITH maxValues AS (SELECT {finalTable}.* FROM {finalTable} JOIN (
    SELECT site_id, max(data_value) AS max_stage FROM {finalTable} GROUP BY site_id
) AS max_stage_q ON {finalTable}.site_id = max_stage_q.site_id AND {finalTable}.data_value = max_stage_q.max_stage)

INSERT INTO {maxTable} SELECT maxValues.* FROM maxValues JOIN (
    SELECT site_id, max(data_time) AS max_time FROM maxValues GROUP BY site_id
) AS maxTimes ON maxValues.site_id = maxTimes.site_id AND maxValues.data_time = maxTimes.max_time""".format(finalTable=finalDataTable, maxTable=maxDataTable)

    sqlUpdateEventsTable = """INSERT INTO EVENTS (event_name, start_date, end_date, total_readings, gages_read, readings_table, max_table, createdDate, lastUpdated)
  VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?) 
  ON CONFLICT(event_name) 
  DO UPDATE SET start_date=excluded.start_date, end_date=excluded.end_date, lastUpdated=excluded.lastUpdated, total_readings=excluded.total_readings, gages_read=excluded.gages_read;"""

    for day in range (0, days + 1):
        currentRangeDate = start_date + timedelta(days=day)

        baseURL = 'https://meckgisdev.mecklenburgcountync.gov/api/contrail?method=GetSensorData&system=c57f3913-ac01-4aa7-b633-e8311f45f74a'

        timeZoneString = "&tz=us/eastern"

        for hour in range(0, 24, 2):

            startDateString = "&data_start=" + str(currentRangeDate) + "%20" + str(hour).zfill(2) + ":00:00"
            endDateString = "&data_end=" + str(currentRangeDate) + "%20" + str(hour + 1).zfill(2) + ":59:59"

            queryURL = baseURL + startDateString + endDateString + timeZoneString

            try:
                response = requests.get(queryURL)
                
                if (response):

                    responseText = response.text
                    row = ET.fromstring(responseText)
                    
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

                        gageData = (site_id, sensor_id, data_time, stage)
                        
                        try:
                            cur.execute(sqlInsert, gageData)
                        except sqlite3.Error as error:
                            updateLogMessage("Error writing to database: " + str(error))
                else:
                     updateLogMessage("API error for datetime block: " + str(currentRangeDate) + " " + str(hour))

            except requests.exceptions.RequestException as e:
                errorMessage = "API error: " + str(e)
                updateLogMessage(errorMessage)

    try:        
        cur.execute(sqlFinalJoin)
        cur.execute("SELECT COUNT(*) FROM " + finalDataTable)
        totalReadings = cur.fetchone()[0]
    except sqlite3.Error as error:
        updateLogMessage("Error writing final data: " + error)
 
    try:
        cur.execute(sqlMaxJoin)
        cur.execute("SELECT COUNT(*) FROM " + maxDataTable)
        gagesRead = cur.fetchone()[0]
    except sqlite3.Error as error:
        updateLogMessage("Error writing max data: " + error)


    eventData = (eventName, start_date, end_date, totalReadings, gagesRead, finalDataTable, maxDataTable, run_datetime, run_datetime)

    try:
        cur.execute(sqlUpdateEventsTable, eventData)
    except sqlite3.Error as error:
        updateLogMessage("Error writing event data: " + error)


    sqlInsertLog = """INSERT INTO eventLogs (event_name, start_date, end_date, attempt_dateTime, log_Message)
  VALUES(?,?,?,?,?,?,?)"""

    logData = (eventName, start_date, end_date, run_datetime, logMessage)

    try:
        cur.execute(sqlInsertLog, logData)
    except sqlite3.Error as error:
        updateLogMessage("Error writing log: " + error)


    logEventOutcome()

    conn.commit()
    conn.close()    


getEventData((2020, 8, 21), (2020, 8, 23), "testEvents2")


print('start time', start_time)
print("--- %s seconds ---" % (time.time() - start_time))

