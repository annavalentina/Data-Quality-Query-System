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
from Project import Project
from AccuracyExpression import AccuracyExpression
from Accuracy import Accuracy
import numpy as np
import pandas as pd
################################################################################
# This query deals with the accuracy operator that assigns a score to each row 
# and can appear in several forms.
# (Only for the business data files)
#################################################################################
class QueryA():
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
    logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )
    filePath=sys.argv[1] # Path for the files
    outFilePath=sys.argv[2] # Path for the output file
    typeOfData=sys.argv[3] # Type of dataset
    with open(outFilePath, "a") as myfile: # Open output file
       myfile.write("QueryA_business\n")
    timing = Timing(outFilePath)
    startTime = timing.startTime() # Start measuring time

    # Create a dataframe from a file
    inputTable_1 = 'orderT_11rows'
    predScan_left = None
    selScan_left = ScanSelect(inputTable_1, predScan_left, filePath,typeOfData)
    outputScan_left = selScan_left.execute()

    # Find Accuracy score for each row
    accuracyAttr = "Accuracy"
    accInputExpr = AccuracyExpression(outputScan_left,"ship_date",">","submit_date")
    accuracyOp = Accuracy(outputScan_left, accuracyAttr, accInputExpr)
    outputAccuracy = accuracyOp.execute()

    # Select columns from the dataframe
    attrList = ["order_no","Accuracy_score"]
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
