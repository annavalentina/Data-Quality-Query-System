from Timing import Timing
from ScanSelect import ScanSelect
from Project import Project
from RowCompleteness import RowCompleteness
import numpy as np
import pandas as pd
import sys
################################################################################
# This query computes a completeness score for each record regarding the fields 
# defined by the user. In order to establish whether a value is missing, apart
# from blanks, the operator checks the content of the corresponding cells against 
# special symbols used to denoted missing values (e.g., 0 for some numeric fields).
# (Only for the business data files)
#################################################################################
class QueryC():
    filePath=sys.argv[1] # Path for the files
    outFilePath=sys.argv[2] # Path for the output file
    typeOfData=sys.argv[3] # Type of dataset
    with open(outFilePath, "a") as myfile: # Open output file
       myfile.write("QueryC_business\n")
    timing = Timing(outFilePath)
    startTime = timing.startTime() # Start measuring time
	
    # Create a dataframe from a file
    inputTable_1 = 'orderT_11rows'
    predScan_left = None
    selScan_left = ScanSelect(inputTable_1, predScan_left,filePath,typeOfData)
    outputScan_left = selScan_left.execute()

    # Find Completeness score for each row
    attrName = "Completeness"
    inputColumnNames = ["ship_date","statusOrder"]
    inputSymbols = ['empty','empty'] 
    completenessOp = RowCompleteness(outputScan_left, attrName, inputColumnNames, inputSymbols)
    outputCompleteness = completenessOp.execute()
	
    # Select columns from the dataframe
    attrList = ["order_no","Completeness_score"]
    proj = Project(outputCompleteness, attrList)
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
