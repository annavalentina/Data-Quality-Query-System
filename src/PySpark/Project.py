################################################################################
# Extracts columns from a dynamic generic array based on a list of attributes 
# given (name of the columns).
#################################################################################
class Project():
	def __init__(self,inputInput,inputAttrList):
		self.input = inputInput
		self.attrList = inputAttrList
		self.output = []
		
	def execute(self):
		dfRes = self.input.select(self.attrList)
		return dfRes
