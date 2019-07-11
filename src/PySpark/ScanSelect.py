import csv
from datetime import datetime
from pyspark.sql.functions import unix_timestamp, col
from pyspark.sql.types import TimestampType
################################################################################
# Loads the dataset from a CSV file and creates dataframes with the appropriate 
# information given by the dataset values.
#################################################################################
class ScanSelect():

	def __init__(self, inputTableName, inputPredicate, inputSqlContext,filePath,typeOfData):
		self.tableName = inputTableName
		self.predicate = inputPredicate
		self.filePath=filePath
		self.sqlContext = inputSqlContext
		self.typeOfData=typeOfData

		
	def execute(self):
		
		loadFile = self.filePath+self.tableName+".csv"
		dfInput= self.sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(loadFile)# Load file

        # eBusiness Data Files for all Queries:
		if(self.typeOfData=='Business'):
			if (self.tableName == 'orderT_11rows'):                       
				dfInput= dfInput.select(
				dfInput.order_no.cast("int").alias("order_no"),
				dfInput.customer_id.cast("int").alias("customer_id"),
				dfInput.product_id.cast("int").alias("product_id"),
				dfInput.quantity.cast("int").alias("quantity"),
				unix_timestamp(dfInput.submit_date, 'yyyy-MM-dd' 'HH:mm:ss').cast("timestamp").alias("submit_date"),
				unix_timestamp(dfInput.ship_date, 'yyyy-MM-dd' 'HH:mm:ss').cast("timestamp").alias("ship_date"),
				dfInput.statusTimeliness_id.cast("int").alias("statusTimeliness_id"),
				dfInput.statusOrder)

			elif (self.tableName == 'statusTimelinessQR_11rows'):                    
 				dfInput= dfInput.select(
				dfInput.statusTimeliness_qid.cast("int").alias("statusTimeliness_qid"),
				unix_timestamp(dfInput.lastUpdateTime, 'yyyy-MM-dd' 'HH:mm:ss').cast("timestamp").alias("lastUpdateTime"),
				unix_timestamp(dfInput.expiryTime, 'yyyy-MM-dd' 'HH:mm:ss').cast("timestamp").alias("expiryTime"),
				unix_timestamp(dfInput.deliveryTime, 'yyyy-MM-dd' 'HH:mm:ss').cast("timestamp").alias("deliveryTime"),
				dfInput.age.cast("int").alias("age"))
			elif (self.tableName == 'testeBusinessData_customerInfo'):                    
				nameAttributes = ["custom_id", "name", "type", "credit", "houseNo", "postcode", "phone", "email"]
				dfInput= dfInput.select(
				dfInput.custom_id.cast("int").alias("custom_id"),
				dfInput.name,
				dfInput.type,
				dfInput.credit.cast("int").alias("credit"),
				dfInput.houseNo.cast("int").alias("houseNo"))

        # Traffic Data Files for all Queries:
		elif(self.typeOfData=='Traffic'):
			if(self.tableName == 'newColpvr_2016-01-01_366d_11rows'):
                    	    dfInput= dfInput.select(
				unix_timestamp(dfInput.Sdate, 'dd/MM/yy' 'HH:mm').cast("timestamp").alias("Sdate"),
				dfInput.Cosit.cast("int").alias("Cosit"),
				dfInput.LaneNumber.cast("int").alias("LaneNumber"),
				dfInput.LaneDescription,
				dfInput.LaneDirection.cast("int").alias("LaneDirection"),
				dfInput.DirectionDescription,
				dfInput.Volume.cast("int").alias("Volume"),
                        	dfInput.Flags.cast("int").alias("Flags"),
                        	dfInput.FlagText,
                        	dfInput.AvgSpeed.cast("int").alias("AvgSpeed"),
                        	dfInput.PmlHGV.cast("int").alias("PmlHGV"),
                        	dfInput.Class1Volume.cast("int").alias("Class1Volume"),
                        	dfInput.Class2Volume.cast("int").alias("Class2Volume"),
                        	dfInput.Class3Volume.cast("int").alias("Class3Volume"),
                        	dfInput.Class4Volume.cast("int").alias("Class4Volume"),
                        	dfInput.Class5Volume.cast("int").alias("Class5Volume"),
                        	dfInput.Class6Volume.cast("int").alias("Class6Volume"),
                        	dfInput.VolumeTimeliness_id.cast("int").alias("VolumeTimeliness_id"))
			elif(self.tableName == 'TfGM_completeTuple_VolumeTimelinessQR_11rows'):
                       		dfInput= dfInput.select(
				dfInput.VolumeTimeliness_qid.cast("int").alias("VolumeTimeliness_qid"),
				unix_timestamp(dfInput.lastUpdateTime, 'yyyy-MM-dd' 'HH:mm:ss').cast("timestamp").alias("lastUpdateTime"),
				unix_timestamp(dfInput.expiryTime, 'yyyy-MM-dd' 'HH:mm:ss').cast("timestamp").alias("expiryTime"),
				unix_timestamp(dfInput.deliveryTime, 'yyyy-MM-dd' 'HH:mm:ss').cast("timestamp").alias("deliveryTime"),
				dfInput.age.cast("int").alias("age"))
			elif(self.tableName == 'testTrafficData_latlongInfo'):
                       		dfInput= dfInput.select(
				dfInput.Site_id.cast("int").alias("Site_id"),
				dfInput.Latitude.cast("float").alias("Latitude"),
				dfInput.Longitude.cast("float").alias("Longitude"))

		# Numeric Data Files for all Queries:
		elif(self.typeOfData=='Numeric'):
			if(self.tableName == 'newColpvr_2016-01-01_366d_11rows'):
                    	    dfInput= dfInput.select(
				unix_timestamp(dfInput.Sdate, 'dd/MM/yy' 'HH:mm').cast("timestamp").alias("Sdate"),
				dfInput.Cosit.cast("int").alias("Cosit"),
				dfInput.LaneNumber.cast("int").alias("LaneNumber"),
				dfInput.LaneDirection.cast("int").alias("LaneDirection"),
				dfInput.Volume.cast("int").alias("Volume"),
                        	dfInput.Flags.cast("int").alias("Flags"),
                        	dfInput.AvgSpeed.cast("int").alias("AvgSpeed"),
                        	dfInput.PmlHGV.cast("int").alias("PmlHGV"),
                        	dfInput.Class1Volume.cast("int").alias("Class1Volume"),
                        	dfInput.Class2Volume.cast("int").alias("Class2Volume"),
                        	dfInput.Class3Volume.cast("int").alias("Class3Volume"),
                        	dfInput.Class4Volume.cast("int").alias("Class4Volume"),
                        	dfInput.Class5Volume.cast("int").alias("Class5Volume"),
                        	dfInput.Class6Volume.cast("int").alias("Class6Volume"),
                        	dfInput.VolumeTimeliness_id.cast("int").alias("VolumeTimeliness_id"))
			elif(self.tableName == 'TfGM_completeTuple_VolumeTimelinessQR_11rows'):
                       		dfInput= dfInput.select(
				dfInput.VolumeTimeliness_qid.cast("int").alias("VolumeTimeliness_qid"),
				unix_timestamp(dfInput.lastUpdateTime, 'yyyy-MM-dd' 'HH:mm:ss').cast("timestamp").alias("lastUpdateTime"),
				unix_timestamp(dfInput.expiryTime, 'yyyy-MM-dd' 'HH:mm:ss').cast("timestamp").alias("expiryTime"),
				unix_timestamp(dfInput.deliveryTime, 'yyyy-MM-dd' 'HH:mm:ss').cast("timestamp").alias("deliveryTime"),
				dfInput.age.cast("int").alias("age"))
			elif(self.tableName == 'testTrafficData_latlongInfo'):
                       		dfInput= dfInput.select(
				dfInput.Site_id.cast("int").alias("Site_id"),
				dfInput.Latitude.cast("float").alias("Latitude"),
				dfInput.Longitude.cast("float").alias("Longitude"))

		# Categorical Data Files for all Queries:
		elif(self.typeOfData=='Categorical'):
			if(self.tableName == 'newColpvr_2016-01-01_366d_11rows'):
                    	    dfInput= dfInput.select(
				unix_timestamp(dfInput.Sdate, 'dd/MM/yy' 'HH:mm').cast("timestamp").alias("Sdate"),
				dfInput.Cosit.cast("int").alias("Cosit"),
				dfInput.LaneDirection.cast("int").alias("LaneDirection"),
				dfInput.DirectionDescription,
				dfInput.Volume,
                        	dfInput.FlagText,
                        	dfInput.AvgSpeed,
                        	dfInput.Class1Volume,
                        	dfInput.Class2Volume,
                        	dfInput.Class3Volume,
                        	dfInput.Class4Volume,
                        	dfInput.Class5Volume,
                        	dfInput.Class6Volume,
                        	dfInput.VolumeTimeliness_id.cast("int").alias("VolumeTimeliness_id"))
			elif(self.tableName == 'TfGM_completeTuple_VolumeTimelinessQR_11rows'):
                       		dfInput= dfInput.select(
				dfInput.VolumeTimeliness_qid.cast("int").alias("VolumeTimeliness_qid"),
				unix_timestamp(dfInput.lastUpdateTime, 'yyyy-MM-dd' 'HH:mm:ss').cast("timestamp").alias("lastUpdateTime"),
				unix_timestamp(dfInput.expiryTime, 'yyyy-MM-dd' 'HH:mm:ss').cast("timestamp").alias("expiryTime"),
				unix_timestamp(dfInput.deliveryTime, 'yyyy-MM-dd' 'HH:mm:ss').cast("timestamp").alias("deliveryTime"),
				dfInput.age.cast("int").alias("age"))
			elif(self.tableName == 'testTrafficData_latlongInfo'):
                       		dfInput= dfInput.select(
				dfInput.Site_id.cast("int").alias("Site_id"),
				dfInput.Latitude.cast("float").alias("Latitude"),
				dfInput.Longitude.cast("float").alias("Longitude"))
		

		


		if (self.predicate != None):
			dfDiscardTuple = self.predicate.apply(dfInput)
			dfOutput = dfDiscardTuple
			
		else: 
			dfOutput = dfInput
		return dfOutput
