import sqlite3
from sqlite3 import Error
import time
import requests
#import schedule
import arcpy
from arcpy.sa import *
import numpy

start_time = time.time()
arcpy.env.workspace = r'C:\Users\tzenebe\App\RARRDatabase\_SourceData\RARR_Inundation.gdb'
arcpy.env.overwriteOutput = True
arcpy.env.cellSize = "MINOF" # Set the cell size environment using --this is good to determin the out put of the raster.
try:
    if arcpy.CheckExtension("Spatial") == "Available":
        arcpy.CheckOutExtension("Spatial")
        print "Checked out \"Spatial\" Extension"
    else:
        raise LicenseError
except LicenseError:
    print "Spatial Analyst license is unavailable"
except:
    print arcpy.GetMessages(2)
    print 'hello'
#sensorsFt =  arcpy.GetParameterAsText(0)
fields = ['SITE_ID','DATA_TIME', 'MSL_FT']
field2 = ['CurrentTR','MSL_FT', 'WSE002YR', 'WSE005YR', 'WSE010YR', 'WSE025YR', 'WSE050YR', 'WSE100YR', 'WSE500YR', 'SITE_ID', 'syntheticGageRefID']
#in case if we need error report--- for now not used
def create_connection(db_file):
   
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn
conn = sqlite3.connect(r'C:\Users\tzenebe\Documents\ArcGIS\api-call-using-python\rarr.db')

db = conn.cursor()
count = 0
M = 15 + count
# Create update cursor for feature class 
# arcpy.AddMessage('update the gauge table with the Instantaneous water level reading...')
# with arcpy.da.UpdateCursor('RARR_Gages106_Pro', fields) as cursor:
#     for row in cursor:
#         SiteID = row[0]
#         db.execute("SELECT * FROM sensorData WHERE site_id=?", (SiteID,))
#         #db.execute("SELECT * FROM sept_17_max WHERE site_id=?", (SiteID,)) # get data from the view table
#         sensorDataRow = db.fetchone()
#         if sensorDataRow is not None:
#             row[1] = sensorDataRow[2]
#             row[2] = sensorDataRow[3] + M # 15 feet added
#             cursor.updateRow(row)
arcpy.AddMessage('update the synthetic gauges data from the nearby measured sensore reading...')
# with arcpy.da.UpdateCursor('RARR_Gages106_Pro',field2, "SITE_ID = 'Synthetic'") as cursor:
#     for row in cursor:
#         ID = row[10]
#         synRow = arcpy.da.SearchCursor('RARR_Gages106_Pro', field2, "SITE_ID = '{}'".format(ID)).next()
#         row[1] = synRow[1]
#         row[2] = synRow[2]
#         row[3] = synRow[3]
#         row[4] = synRow[4]
#         row[5] = synRow[5]
#         row[6] = synRow[6]
#         row[7] = synRow[7]
#         row[8] = synRow[8]
#         cursor.updateRow(row)
arcpy.AddMessage('update the gauge table IR value...')
# with arcpy.da.UpdateCursor('RARR_Gages106_Pro', field2) as cursor:
#     for row in cursor:
#         #'WSE002YR', 'WSE005YR', 'WSE010YR', 'WSE025YR', 'WSE050YR', 'WSE100YR', 'WSE500YR'
#         if row[9] == 'TestGage':
#             row[0] = 9999
#             cursor.updateRow(row)
#         elif row[1] is None:
#             row[0] = None
#             cursor.updateRow(row)
#         else:
#             msl = row[1]
#             #print msl
#             minMSL = row[2] # two year 
#             maxMSL = row[8] # 100 year
#             if minMSL > msl:
#                 #no flooding
#                 row[0] = 9999
#                 cursor.updateRow(row)
#             elif maxMSL < msl: # need some work for now put 0.2
#                 #Verry high flooding
#                 row[0] = 0.2
#                 cursor.updateRow(row)
#             else:
#                 #We may add one more check if the msl is equals to the value of the known WSE --might help to reduce processing time when we get msl equals to known msl
#                 #flooding happend so get the interpolated RI
#                 obj = {row[2]: 50, row[3]: 20, row[4]: 10, row[5]:4, row[6]: 2, row[7]: 1, row[8]: 0.2}
#                 #print obj
#                 newRow = row[2:9]
#                 print newRow
#                 numpyRow = numpy.array(newRow)
#                 upperMSL = numpyRow[numpyRow > msl].min() 
#                 #print upperMSL
#                 lowerMSL = numpyRow[numpyRow < msl].max() 
#                 upperRI = obj[upperMSL]
#                 lowerRI = obj[lowerMSL]
#                 RI = lowerRI + ((msl - lowerMSL)*((upperRI - lowerRI)/(upperMSL - lowerMSL)))
#                 row[0] = RI
#                 cursor.updateRow(row)

# Local variables:
AreaLine = r"C:\Users\tzenebe\App\RARRDatabase\_SourceData\DMKPlanning\DMKPlanning.gdb\AreaLine"
rIDW = "C:\\Users\\tzenebe\\App\\RARRDatabase\\_SourceData\\DMKPlanning\\IDWRasters.gdb\\IDWWorking"
rPAC = "C:\\Users\\tzenebe\\App\\RARRDatabase\\_SourceData\\DMKPlanning\\MeckPAC_RARR.gdb\\rPAC" 

# Process: IDW
arcpy.AddMessage('RI raster production on progress')
IDWWorking = Idw('RARR_Gages106_Pro', "CurrentTR", "100", "2", RadiusVariable(12, 150000), AreaLine)
IDWWorking.save("C:\\Users\\tzenebe\\App\\RARRDatabase\\_SourceData\\DMKPlanning\\IDWRasters.gdb\\IDWWorking")
arcpy.AddMessage('RI raster production completed')
# Process: Raster Calculator 
arcpy.AddMessage('Inundation map production in progress')
rasterIn1= Raster(rIDW)
rasterIn2=Raster(rPAC)
SampleEvent = Con((rasterIn1 - rasterIn2) <= 0, 1)
SampleEvent.save("C:\\Users\\tzenebe\\App\\RARRDatabase\\_SourceData\\DMKPlanning\\Mappingtest.gdb\\SampleEvent")
arcpy.AddMessage('Inundation map production completed')

# Process: Raster copy
# arcpy.AddMessage('copy inundation map production started')
# out_Rater = "rasterAt" + str(round(start_time))[:-2] 
# out_Feature = "gageFturAt" + str(round(start_time))[:-2] 
# in_raster = "C:\\Users\\tzenebe\\App\\RARRDatabase\\_SourceData\\DMKPlanning\\Mappingtest.gdb\\SampleEvent"
# arcpy.CopyRaster_management(in_raster, out_Rater)
# arcpy.CopyFeatures_management("RARR_Gages106_Pro", out_Feature)

print('start time', start_time)
print("--- %s seconds ---" % (time.time() - start_time))
arcpy.AddMessage('completed as needed')

# schedule.every(2).minutes.do(runModelPy)

# while True:
#     count = count + 5
#     schedule.run_pending()
#     time.sleep(1)
#conn.commit()

#conn.close()
