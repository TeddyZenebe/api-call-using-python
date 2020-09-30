---------------------------------------------------------------------------

# Import arcpy module
import arcpy

# Script arguments
SampleEvent = arcpy.GetParameterAsText(0)
if SampleEvent == '#' or not SampleEvent:
    SampleEvent = "C:\\Users\\tzenebe\\App\\RARRDatabase\\_SourceData\\DMKPlanning\\Mappingtest.gdb\\SampleEvent" # provide a default value if unspecified

rPAC = arcpy.GetParameterAsText(1)
if rPAC == '#' or not rPAC:
    rPAC = "C:\\Users\\tzenebe\\App\\RARRDatabase\\_SourceData\\DMKPlanning\\MeckPAC_RARR.gdb\\rPAC" # provide a default value if unspecified

plyIDW_Boundary__Line = arcpy.GetParameterAsText(2)
if plyIDW_Boundary__Line == '#' or not plyIDW_Boundary__Line:
    plyIDW_Boundary__Line = "C:\\Users\\tzenebe\\App\\RARRDatabase\\_SourceData\\DMKPlanning\\DMKPlanning.gdb\\plyIDW_Boundary__Line" # provide a default value if unspecified

RARR_Gages106__2_ = arcpy.GetParameterAsText(3)
if RARR_Gages106__2_ == '#' or not RARR_Gages106__2_:
    RARR_Gages106__2_ = "RARR_Gages106" # provide a default value if unspecified

# Local variables:
IDWWorking = "C:\\Users\\tzenebe\\App\\RARRDatabase\\_SourceData\\DMKPlanning\\IDWRasters.gdb\\IDWWorking"
Cell_Size = "MINOF"

# Process: IDW
arcpy.gp.Idw_sa(RARR_Gages106__2_, "CurrentTR", IDWWorking, "100", "2", "VARIABLE 12", plyIDW_Boundary__Line)

# Process: Raster Calculator
tempEnvironment0 = arcpy.env.cellSize
arcpy.env.cellSize = Cell_Size
arcpy.gp.RasterCalculator_sa("Con(\"%IDWWorking%\" - \"%rPAC%\" <0,1)", SampleEvent)
arcpy.env.cellSize = tempEnvironment0

