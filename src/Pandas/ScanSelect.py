import csv
import numpy as np
import pandas as pd
from datetime import datetime
################################################################################
# Loads the dataset from a CSV file and creates dataframes with the appropriate 
# information given by the dataset values.
#################################################################################
class ScanSelect():

	def __init__(self, inputTableName, inputPredicate,filePath,typeOfData):
		self.tableName = inputTableName
		self.predicate = inputPredicate
		self.filePath = filePath
		self.typeOfData=typeOfData
		self.output = []
		
	def execute(self):
        # eBusiness Data Files for all Queries:
		if(self.typeOfData=='Business'):
			if (self.tableName == 'orderT_11rows'):                       
				nameAttributes = ["order_no", "customer_id", "product_id", "quantity", "submit_date", "ship_date", "statusTimeliness_id", "statusOrder"]
				dtypes = { "order_no":'int32', "customer_id":'int32',"product_id":'int32',"quantity":'int32',"statusTimeliness_id":'int32'}

			elif (self.tableName == 'statusTimelinessQR_11rows'):                    
				nameAttributes = ["statusTimeliness_qid", "lastUpdateTime", "expiryTime", "deliveryTime", "age"]
				dtypes = { "statusTimeliness_qid":'int32',"age":'int8'}

			elif (self.tableName == 'testeBusinessData_customerInfo'):                    
				nameAttributes = ["custom_id", "name", "type", "credit", "houseNo", "postcode", "phone", "email"]
				dtypes = { "custom_id":'int16', "name":'int8',"credit":'int8',"houseNo":'category',"phone":'int16'}

        # Traffic Data Files for all Queries:
		elif(self.typeOfData=='Traffic'):
			if(self.tableName == 'newColpvr_2016-01-01_366d_11rows'):
				nameAttributes = ["Sdate", "Cosit", "LaneNumber", "LaneDescription", "LaneDirection", "DirectionDescription", "Volume", "Flags", "FlagText", "AvgSpeed", "PmlHGV", "Class1Volume", "Class2Volume", "Class3Volume", "Class4Volume", "Class5Volume", "Class6Volume", "VolumeTimeliness_id"]
				dtypes = { "Cosit":'float16', "LaneNumber":'int8',"LaneDescription":'category',"LaneDirection":'int8',"DirectionDescription":'category',"Volume":'float16',"Flags":'int8',"FlagText":'category',"AvgSpeed":'int32',"PmlHGV":'int16',"Class1Volume":'float16',"Class2Volume":'float16',"Class3Volume":'int8',"Class4Volume":'int8',"Class5Volume":'int8',"Class6Volume":'int8',"VolumeTimeliness_id":'int32'}
			elif(self.tableName == 'TfGM_completeTuple_VolumeTimelinessQR_11rows'):
				nameAttributes = ["VolumeTimeliness_qid", "lastUpdateTime", "expiryTime", "deliveryTime", "age"]
				dtypes = {"VolumeTimeliness_qid":'int32',"age":'int8'}
			elif(self.tableName == 'testTrafficData_latlongInfo'):
				nameAttributes = ["Site_id", "Latitude", "Longitude"]
				dtypes = {"Site_id":'float16',"Latitude":'float32',"Longitude":'float32'}

        # Numeric Data Files for all Queries:
		elif(self.typeOfData=='Numeric'):
			if(self.tableName == 'newColpvr_2016-01-01_366d_11rows'):
				nameAttributes = ["Sdate", "Cosit", "LaneNumber", "LaneDirection", "Volume", "Flags", "AvgSpeed", "PmlHGV", "Class1Volume", "Class2Volume", "Class3Volume", "Class4Volume", "Class5Volume", "Class6Volume", "VolumeTimeliness_id"]
				dtypes = { "Cosit":'float16', "LaneNumber":'int8',"LaneDirection":'int8',"Volume":'float16',"Flags":'int8',"AvgSpeed":'int32',"PmlHGV":'int16',"Class1Volume":'float16',"Class2Volume":'float16',"Class3Volume":'int8',"Class4Volume":'int8',"Class5Volume":'int8',"Class6Volume":'int8',"VolumeTimeliness_id":'int32'}
			elif(self.tableName == 'TfGM_completeTuple_VolumeTimelinessQR_11rows'):
				nameAttributes = ["VolumeTimeliness_qid", "lastUpdateTime", "expiryTime", "deliveryTime", "age"]
				dtypes = {"VolumeTimeliness_qid":'int32',"age":'int8'}
			elif(self.tableName == 'testTrafficData_latlongInfo'):
				nameAttributes = ["Site_id", "Latitude", "Longitude"]
				dtypes = {"Site_id":'float16',"Latitude":'float32',"Longitude":'float32'}

        # Categorical Data Files for all Queries:
		elif(self.typeOfData=='Categorical'):
			if(self.tableName == 'newColpvr_2016-01-01_366d_11rows'):
				nameAttributes = ["Sdate", "Cosit", "LaneDirection", "DirectionDescription", "Volume", "FlagText", "AvgSpeed",  "Class1Volume", "Class2Volume", "Class3Volume", "Class4Volume", "Class5Volume", "Class6Volume", "VolumeTimeliness_id"]
				dtypes = { "Cosit":'float16',"LaneDirection":'category',"DirectionDescription":'category',"Volume":'category',"FlagText":'category',"AvgSpeed":'category',"Class3Volume":'category',"Class4Volume":'category',"Class5Volume":'category',"Class6Volume":'category',"VolumeTimeliness_id":'int32'}
			elif(self.tableName == 'TfGM_completeTuple_VolumeTimelinessQR_11rows'):
				nameAttributes = ["VolumeTimeliness_qid", "lastUpdateTime", "expiryTime", "deliveryTime", "age"]
				dtypes = {"VolumeTimeliness_qid":'int32',"age":'int8'}
			elif(self.tableName == 'testTrafficData_latlongInfo'):
				nameAttributes = ["Site_id", "Latitude", "Longitude"]
				dtypes = {"Site_id":'float16',"Latitude":'float32',"Longitude":'float32'}
		
		dfInput = pd.read_csv(self.filePath+self.tableName+'.csv', names=nameAttributes,dtype=dtypes,header=0,  encoding='latin-1')# Load file
		
		if (self.predicate != None):
			discardTuple = self.predicate.apply(dfInput)
			dfOutput = discardTuple
			
		else: 
			dfOutput = dfInput

		# Function that measures the memory usage of a Pandas dataframe (for testing purposes)	
		#def mem_usage(pandas_obj):
		#	if isinstance(pandas_obj,pd.DataFrame):
		#		usage_b = pandas_obj.memory_usage(deep=True).sum()
		#	else: # we assume if not a df it's a series
		#		usage_b = pandas_obj.memory_usage(deep=True)
		#	usage_mb = usage_b / 1024 ** 2 # convert bytes to megabytes
		#	return "{:03.2f} MB".format(usage_mb)


		return dfOutput
