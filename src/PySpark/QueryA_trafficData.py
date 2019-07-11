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
################################################################################
# This query deals with the accuracy operator that assigns a score to each row 
# and can appear in several forms.
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
       myfile.write("QueryA_traffic\n")
    timing = Timing(outFilePath)
    startTime = timing.startTime() # Start measuring time

    # Create a dataframe from a file
    inputTable_1 = 'newColpvr_2016-01-01_366d_11rows'  
    predScan_left = None
    selScan_left = ScanSelect(inputTable_1, predScan_left,sqlContext,filePath,typeOfData) 
    outputScan_left = selScan_left.execute() 

    # Find Accuracy score for each row
    accuracyAttr = "Accuracy"
    if(typeOfData!='Categorical'):
     accInputExpr_6 = AccuracyExpression(outputScan_left,"Class5Volume","+","Class6Volume")
     accInputExpr_5 = AccuracyExpression(outputScan_left,"Class4Volume","+",accInputExpr_6)
     accInputExpr_4 = AccuracyExpression(outputScan_left,"Class3Volume","+",accInputExpr_5)
     accInputExpr_3 = AccuracyExpression(outputScan_left,"Class2Volume","+",accInputExpr_4)
     accInputExpr_2 = AccuracyExpression(outputScan_left,"Class1Volume","+",accInputExpr_3)
     accInputExpr_1 = AccuracyExpression(outputScan_left,"Volume","=",accInputExpr_2)
     accuracyOp = Accuracy(outputScan_left, accuracyAttr, accInputExpr_1)
    else:
     accInputExpr_1 = AccuracyExpression(outputScan_left,"Volume","=","Class2Volume")
     accuracyOp = Accuracy(outputScan_left, accuracyAttr, accInputExpr_1)
    outputAccuracy = accuracyOp.execute()
	
    # Select columns from the dataframe
    if(typeOfData!='Categorical'):
     attrList = ["Sdate","LaneNumber","Accuracy_score"]
    else:
     attrList = ["Sdate","Accuracy_score"]
    proj = Project(outputAccuracy, attrList)
    outputFinal = proj.execute()
	
    nrows = outputFinal.count()
	
    stopTime = timing.stopTime()# Stop measuring time
    timing.durationTime(stopTime, startTime)



