import re 
import time
from datetime import datetime
import numpy as np
import pandas as pd
################################################################################
# Calculates the accuracy score of each row based on the user defined rule
#################################################################################
class Accuracy():

	def __init__(self,inputInput,inputAccAttr,inputAccExpr):
		self.input = inputInput
		self.accuracyAttr = inputAccAttr
		self.accExpression = inputAccExpr
		self.output = []
	
	def execute(self):
		accuracyBoolean = self.accExpression.apply() # Call AccuracyExpression file
		attrName = ""+str(self.accuracyAttr)+"_score"
                
        # Add accuracy score as new column
		df = (self.input.assign(added=accuracyBoolean))
		df.ix[df.added == True, "added"] = 100.00  # If accuracy is True set score to 100
		df.ix[df.added == False, "added"] = 0.00   # If accuracy is False set score to 0
        # Rename column containing final timeliness score
		df1 = df.rename(columns={"added": ""+attrName+""})
                
		return df1
