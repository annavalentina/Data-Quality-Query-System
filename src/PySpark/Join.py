from pyspark.sql.functions import col
################################################################################
# Joins two dynamic generic arrays based on a given join predicate.
#################################################################################
class Join(object):#Joins two tables
	def __init__(self,inputInputLeft,inputInputRight,inputPredicate):
		self.inputLeft = inputInputLeft
		self.inputRight = inputInputRight
		self.predicateJoin = inputPredicate
		
	def execute(self):
		if self.predicateJoin != None:
			columnName1 = self.inputLeft.schema.names
			columnName2 = self.inputRight.schema.names
			operand1 = self.predicateJoin[0]
			comparisonOperator = self.predicateJoin[1]
			operand2 = self.predicateJoin[2]
			if (operand1 in columnName1 and operand2 in columnName2 and comparisonOperator == "="):
				dfOutput = self.inputLeft.join(self.inputRight, col(str(self.predicateJoin[0])) == col(str(self.predicateJoin[2])), "left_outer")
			else:
				dfOuput = self.inputLeft.join(self.inputRight)
		else:
			dfOuput = self.inputLeft.join(self.inputRight)
		return dfOutput
