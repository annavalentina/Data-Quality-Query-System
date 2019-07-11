import re 
import time
from datetime import datetime
import numpy as np
import pandas as pd
################################################################################
# Calculates the timeliness score based on formulas
#################################################################################
class Timeliness(): 

	def __init__(self, inputInput, inputTimelinessAttr):
		self.input = inputInput
		self.timelinessAttr = inputTimelinessAttr
		self.output = [] 
	
	def execute(self):
		# Convert timestamp to epoch time in milliseconds
		def unixtime(datetime): 
			epoch = (datetime.astype(np.int64) / 1e6).astype(np.uint64) 
			return epoch
			
		lastUpdateTime_dt = pd.to_datetime(self.input['lastUpdateTime'])
		deliveryTime_dt = pd.to_datetime(self.input['deliveryTime'])
		expiryTime_dt = pd.to_datetime(self.input['expiryTime'])
		
		#Calculate Timeliness Score
		timelinessPartial = (1-  \
		((unixtime(deliveryTime_dt) - unixtime(lastUpdateTime_dt) + self.input['age'])  \
		/(unixtime(expiryTime_dt) - unixtime(lastUpdateTime_dt) + self.input['age'])))
		
		attrName = ""+str(self.timelinessAttr)+"_score"
		
		# Add timeliness score as new column
		df = (self.input.assign(added=timelinessPartial))
		# Apply max(score,0)
		df.ix[df.added < 0.00, "added"] = 0.00
		# Rename column containing final timeliness score
		df1 = df.rename(columns={"added": ""+attrName+""})
		
		return df1

