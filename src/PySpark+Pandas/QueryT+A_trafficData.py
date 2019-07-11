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
from AccuracyExpression import AccuracyExpression
from Accuracy import Accuracy
import numpy as np
import pandas as pd
################################################################################
# This is a complex query that combines two data quality operators, namely 
# timeliness and accuracy, and includes two joins.
#################################################################################
class QueryTA():
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
    logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )
    filePath=sys.argv[1] # Path for the files
    outFilePath=sys.argv[2] # Path for the output file
    typeOfData=sys.argv[3] # Type of dataset
    with open(outFilePath, "a") as myfile: # Open output file
       myfile.write("QueryT+A_traffic\n")
    timing = Timing(outFilePath)
    startTime = timing.startTime() # Start measuring time

    # Create a dataframe from a file
    inputTable_1 = 'newColpvr_2016-01-01_366d_11rows'  
    predScan_left = None
    selScan_left = ScanSelect(inputTable_1, predScan_left,filePath,typeOfData) 
    outputScan_left = selScan_left.execute() 

    # Create a dataframe from a file
    inputTable_2 = 'TfGM_completeTuple_VolumeTimelinessQR_11rows'
    predScan_right = None
    selScan_right = ScanSelect(inputTable_2, predScan_right,filePath,typeOfData)
    outputScan_right = selScan_right.execute()

    # Join two dataframes
    predJoin = ("VolumeTimeliness_id", "=", "VolumeTimeliness_qid")
    dfJoin_1 = Join(outputScan_left, outputScan_right, predJoin)
    outputJoin_1 = dfJoin_1.execute()

    # Find Timeliness score for each row
    timelinessAttr = "timeliness"                    
    timeliness = Timeliness(outputJoin_1, timelinessAttr)
    outputTimeliness = timeliness.execute()

    # Create a dataframe from a file
    inputTable_right_2 = "testTrafficData_latlongInfo"
    predScan_right_2 = None
    selScan_right_2 = ScanSelect(inputTable_right_2, predScan_right_2,filePath,typeOfData)
    outputScan_right_2 = selScan_right_2.execute()

    # Join two dataframes
    predJoin_2 = ("Cosit", "=", "Site_id")
    join_2 = Join(outputTimeliness, outputScan_right_2, predJoin_2)
    outputJoin_2 = join_2.execute()

    # Find Accuracy score for each row
    accuracyAttr = "Accuracy"
    accInputExpr = AccuracyExpression(outputJoin_2,"Latitude",">","Longitude")
    accuracyOp = Accuracy(outputJoin_2, accuracyAttr, accInputExpr)
    outputAccuracy = accuracyOp.execute()

    # Select columns from the dataframe
    attrList = ["VolumeTimeliness_id","timeliness_score","Accuracy_score"]
    proj = Project(outputAccuracy, attrList)
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
