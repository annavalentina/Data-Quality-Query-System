from __future__ import print_function
from pyspark.sql.column import Column, _to_java_column, _to_seq
from pyspark.sql.dataframe import DataFrame
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import sys
import math
from Timing import Timing
from ScanSelect import ScanSelect
from Join import Join
from Timeliness import Timeliness
from Project import Project
from RowCompleteness import RowCompleteness
import numpy as np
import pandas as pd
################################################################################
# This is a complex query that combines two data quality operators, namely 
# timeliness and completeness, and includes two joins.
# (Only for the business data files)
#################################################################################
class QueryTC():
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
    logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )
    filePath=sys.argv[1] # Path for the files
    outFilePath=sys.argv[2] # Path for the output file
    typeOfData=sys.argv[3] # Type of dataset
    with open(outFilePath, "a") as myfile: # Open output file
       myfile.write("QueryT+C_business\n")
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
    selScan_right = ScanSelect(inputTable_2, predScan_right, filePath,typeOfData)
    outputScan_right = selScan_right.execute()

    # Join two dataframes
    predJoin = ("statusTimeliness_id", "=", "statusTimeliness_qid")
    join_1 = Join(outputScan_left, outputScan_right, predJoin)
    outputJoin_1 = join_1.execute()

    # Find Timeliness score for each row
    timelinessAttr = "timeliness"
    timeliness = Timeliness(outputJoin_1, timelinessAttr);
    outputTimeliness = timeliness.execute()

    # Create a dataframe from a file
    inputTable_right_2 = "testeBusinessData_customerInfo"
    predScan_right_2 = None
    selScan_right_2 = ScanSelect(inputTable_right_2, predScan_right_2, filePath,typeOfData)
    outputScan_right_2 = selScan_right_2.execute()

    # Join two dataframes
    predJoin_2 = ("customer_id", "=", "custom_id")
    join_2 = Join(outputTimeliness, outputScan_right_2, predJoin_2)
    outputJoin_2 = join_2.execute()

    # Find Completeness score for each row
    attrName = "Completeness"
    inputColumnNames = ["postcode","email"]
    inputSymbols = [' ',' '] 
    completenessOp = RowCompleteness(outputJoin_2, attrName, inputColumnNames, inputSymbols)
    outputCompleteness = completenessOp.execute()

    # Select columns from the dataframe
    attrList = ["order_no","customer_id","timeliness_score","Completeness_score"]
    proj = Project(outputCompleteness, attrList)
    outputFinal = proj.execute()
	
    nrows = outputFinal.count()
	
    # Uncomment to print final output
    '''
    n = len(outputFinal.index)
    print(outputFinal.head(n).to_string())
    print("Project Output= ")
    print(n)
    '''

    stopTime = timing.stopTime()# Stop measuring time
    timing.durationTime(stopTime, startTime)
