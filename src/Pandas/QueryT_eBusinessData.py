from Timing import Timing
from ScanSelect import ScanSelect
from Join import Join
from Timeliness import Timeliness
from Project import Project
import numpy as np
import pandas as pd
import sys
################################################################################
# This query's distinctive feature is that it inhrenetly includes an expensive join 
# that contains timeliness metadata regarding each data record. 
# (Only for the business data files)
#################################################################################
class QueryT():
    filePath=sys.argv[1] # Path for the files
    outFilePath=sys.argv[2] # Path for the output file
    typeOfData=sys.argv[3] # Type of dataset
    with open(outFilePath, "a") as myfile: # Open output file
       myfile.write("QueryT_business\n")
    timing = Timing(outFilePath)
    startTime = timing.startTime() # Start measuring time

    # Create a dataframe from a file
    inputTable_1 = 'orderT_11rows'
    predScan_left = None
    selScan_left = ScanSelect(inputTable_1, predScan_left,filePath,typeOfData)
    outputScan_left = selScan_left.execute()

    # Create a dataframe from a file
    inputTable_2 = "statusTimelinessQR_11rows"
    predScan_right = None
    selScan_right = ScanSelect(inputTable_2, predScan_right,filePath,typeOfData)
    outputScan_right = selScan_right.execute()

    # Join two dataframes
    predJoin = ("statusTimeliness_id", "=", "statusTimeliness_qid")
    join_1 = Join(outputScan_left, outputScan_right, predJoin)
    outputJoin_1 = join_1.execute()

    # Find Timeliness score for each row
    timelinessAttr = "timeliness"
    timeliness = Timeliness(outputJoin_1, timelinessAttr);
    outputTimeliness = timeliness.execute()

    # Select columns from the dataframe
    attrList = ["statusTimeliness_qid","timeliness_score"]
    proj = Project(outputTimeliness, attrList)
    outputFinal = proj.execute()

    # Uncomment to print final output
    '''
    n = len(outputFinal.index)
    print(outputFinal.head(n).to_string())
    print("Project Output= ")
    print(n)
    '''

    stopTime = timing.stopTime() # Stop measuring time
    timing.durationTime(stopTime, startTime)
