import re 
import time
from datetime import datetime
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
		df = self.accExpression.apply() # Call AccuracyExpression file
		attrName = ""+str(self.accuracyAttr)+"_score"
        # Rename column containing final timeliness score
		df1 = df.withColumnRenamed("accuracy", ""+attrName+"").drop("result")
		return df1
