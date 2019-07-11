import re 
import time
from datetime import datetime
import numpy as np
import pandas as pd
################################################################################
# Calculates the completeness score of each row. The more missed values the row 
# has the less score it will be assigned.
#################################################################################
class RowCompleteness():

    def __init__(self, inputInput, inputCompletenessAttr, inputColumnNames, inputSymbols):
        self.input = inputInput
        self.completenessAttribute = inputCompletenessAttr # Name of attribute to be added to each input record (passed as a String)
        self.columnNames = inputColumnNames # Names of columns whose completeness score is to be calculated (passed as a Series of Strings)
        self.symbols = inputSymbols # Symbols that represent a missing value in the domain of the each corresponging column whose 
		                            #completeness is to be calculated (passed as a series of values)
        self.output = []
        
    def execute(self):
        def completeness(dividend_series, divisor): # Find completeness score for each row
            return dividend_series / divisor


        relevant_columns_df = self.input[self.columnNames] # Extract only columns that will be checked for completeness
        
        nAttributes = len(self.columnNames)
        boolean_df = relevant_columns_df.fillna("empty").eq(self.symbols) # Set missing values to true
        transposed_boolean_df = boolean_df.transpose()
        sumOfTrueValues = transposed_boolean_df.sum() # +1 for each True value
		
        if (sumOfTrueValues.sum()==0): # If no value is missing
            completenessScore_series = 1
        else:
            completenessScore_series = 1 - (completeness((sumOfTrueValues), nAttributes))

        attrName = ""+str(self.completenessAttribute)+"_score"
        # Add completeness score as new column
        df = (self.input.assign(added=completenessScore_series))
        # Rename column containing final RowCompleteness score
        df1 = df.rename(columns={"added": ""+attrName+""})


        return df1
