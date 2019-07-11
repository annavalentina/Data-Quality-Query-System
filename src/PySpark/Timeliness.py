import re 
import time
from datetime import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql.column import Column, _to_java_column, _to_seq
from pyspark.sql import functions as F
################################################################################
# Calculates the timeliness score based on formulas
#################################################################################
class Timeliness():
	def __init__(self,inputInput,inputTimelinessAttr):
		self.input = inputInput
		self.timelinessAttr = inputTimelinessAttr
		
	def execute(self):
		# Convert timestamp to epoch time in milliseconds	
		def unix_timestamp(timestamp=None, format='yyyy-MM-dd HH:mm:ss'):
			sc = SparkContext._active_spark_context
			if timestamp is None:
				return Column(sc._jvm.functions.unix_timestamp())
			return Column(sc._jvm.functions.unix_timestamp(_to_java_column(timestamp), format))
		
		#Calculate Timeliness Score
		timelinessScore = (1 - \
		(((unix_timestamp(self.input.deliveryTime) - unix_timestamp(self.input.lastUpdateTime)) + self.input.age) / \
		((unix_timestamp(self.input.expiryTime) - unix_timestamp(self.input.lastUpdateTime)) + self.input.age) ))
		
		# Add timeliness score as new column
		attrName = ""+str(self.timelinessAttr)+"_score"
		# Apply max(score,0)
		dfOutput = self.input.withColumn(attrName, F.when(timelinessScore >= 0.00, timelinessScore).otherwise(0.00))
		return dfOutput
