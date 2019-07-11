from __future__ import print_function,division
from pyspark.sql.column import Column, _to_java_column, _to_seq
from pyspark.sql.dataframe import DataFrame
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType,BooleanType,FloatType
import re 
import time
from datetime import datetime
################################################################################
# Calculates the completeness score of each row. The more missed values the row 
# has the less score it will be assigned.
#################################################################################
class RowCompleteness():

    def __init__(self, inputInput, inputCompletenessAttr, inputColumnNames, inputSymbols):
        self.input = inputInput
        self.completenessAttribute = inputCompletenessAttr # name of attribute to be added to each input record (passed as a String) 
        self.columnNames = inputColumnNames # names of columns whose completeness score is to be calculated (passed as a Series of Strings)
        self.symbols = inputSymbols # symbols that represent a missing value in the domain of the each corresponging column whose 
		                            #completeness is to be calculated (passed as a series of values)
        self.output = []
	
        
    def execute(self):
        def completeness(dividend_series, divisor): # Find the completeness score for each row 
            if (dividend_series==0):
               return divisor / divisor
            else:
               return 1-(dividend_series / divisor)


        def booleanFlag(x):# If value is missing set flag to True (for traffic dataset)
            if x is None:
              z=True
            else:
              z=False
            return z


        def sumOfColumns(x,y): # Add 1 for each value that is missing
            sum=0;
            if(x==True ):
               sum=sum+1;
            if(y==True ):
               sum=sum+1;
            return sum


		# Create user defined functions  
        bf_udf=udf(booleanFlag,BooleanType())
        sc_udf=udf(sumOfColumns,IntegerType())
        comp_udf=udf(completeness,FloatType())
	
        
        nAttributes = len(self.columnNames)
       # relevant_columns_df = self.input[self.columnNames]
	   
	    # Set flags for each column
        boolean_df = self.input.withColumn('temp1', bf_udf(self.input[self.columnNames[0]])).withColumn('temp2', bf_udf(self.input[self.columnNames[1]]))
        # Sum missing values for each row
        sumOfTrueValues=boolean_df.withColumn('columnsSum', sc_udf(boolean_df['temp1'],boolean_df['temp2'])).drop(boolean_df["temp1"]).drop(boolean_df["temp2"])
        
        
        attrName = ""+str(self.completenessAttribute)+"_score"
		# Add new column with the completeness scores
        df = sumOfTrueValues.withColumn(""+attrName+"",comp_udf(sumOfTrueValues["columnsSum"],lit(nAttributes))).drop(sumOfTrueValues["columnsSum"])
        return df
