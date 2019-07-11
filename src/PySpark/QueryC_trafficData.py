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
from Predicate import Predicate
from ScanSelect import ScanSelect
from Join import Join
from Timeliness import Timeliness
from Select import Select
from Project import Project
from AccuracyExpression import AccuracyExpression
from Accuracy import Accuracy
from RowCompleteness import RowCompleteness
import time
################################################################################
# This query computes a completeness score for each record regarding the fields 
# defined by the user. In order to establish whether a value is missing, apart
# from blanks, the operator checks the content of the corresponding cells against 
# special symbols used to denoted missing values (e.g., 0 for some numeric fields).
#################################################################################
class QueryC():
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
    logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )
    filePath=sys.argv[1] # Path for the files
    outFilePath=sys.argv[2] # Path for the output file
    typeOfData=sys.argv[3] # Type of dataset
    with open(outFilePath, "a") as myfile: # Open output file
       myfile.write("QueryC_traffic\n")
    timing = Timing(outFilePath)
    startTime = timing.startTime() # Start measuring time

    # Create a dataframe from a file
    inputTable_1 = 'newColpvr_2016-01-01_366d_11rows'  
    predScan_left = None
    selScan_left = ScanSelect(inputTable_1, predScan_left, sqlContext,filePath,typeOfData) 
    outputScan_left = selScan_left.execute() 

    # Find Completeness score for each row
    attrName = "Completeness"
    inputColumnNames = ["Class1Volume","Class2Volume"] 
    inputSymbols = ['null','null']
    completenessOp = RowCompleteness(outputScan_left, attrName, inputColumnNames, inputSymbols)
    outputCompleteness = completenessOp.execute()

    # Select columns from the dataframe
    if(typeOfData!='Categorical'):
     attrList = ["Sdate","LaneNumber","Completeness_score"]
    else:
     attrList = ["Sdate","Completeness_score"]
    proj = Project(outputCompleteness, attrList)
    outputFinal = proj.execute()

    nrows = outputFinal.count()
	
    stopTime = timing.stopTime() # Stop measuring time
    timing.durationTime(stopTime, startTime)



