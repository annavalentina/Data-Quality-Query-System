import time
################################################################################
# Gets the system time, allowing the measurement of the elapsed time by saving 
# the system time at the beginning and at the end of the query processing.
#################################################################################
class Timing():
    def __init__(self, outFilePath):
        self.outFilePath=outFilePath
    def startTime(self):# Start measuring time   
        queryStartTime = int(round(time.time()* 1e9))
        print("Query start time in nanoseconds = " + str(queryStartTime))
        return queryStartTime
    
    def stopTime(self):# Stop measuring time            
        queryStopTime = int(round(time.time()* 1e9))
        print("Query stop time in nanoseconds = " + str(queryStopTime))
        return queryStopTime 
        
    def durationTime(self,queryStopTime,queryStartTime):# Calculate total elapsed time    
        queryExecutionDuration = (queryStopTime - queryStartTime)
        with open(self.outFilePath, "a") as myfile:
           myfile.write("queryExecutionDuration in nanoseconds: " + str(queryExecutionDuration)+"\n")
           myfile.write("queryExecutionDuration in seconds: " + str(queryExecutionDuration/1000000000)+"\n")
        print("queryExecutionDuration in nanoseconds: " + str(queryExecutionDuration))
        print("queryExecutionDuration in seconds: " + str((queryExecutionDuration/1000000000)))
