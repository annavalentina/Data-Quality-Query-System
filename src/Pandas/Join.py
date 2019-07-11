################################################################################
# Joins two dynamic generic arrays based on a given join predicate.
#################################################################################
class Join():

	def __init__(self, inputInputLeft, inputInputRight, inputPredicate):
		self.inputLeft = inputInputLeft
		self.inputRight = inputInputRight
		self.predicateJoin = inputPredicate
		self.output = []
	
	def execute(self):
		if self.predicateJoin != None:
			columnName1 = self.inputLeft.columns
			columnName2 = self.inputRight.columns
			operand1 = self.predicateJoin[0]
			comparisonOperator = self.predicateJoin[1]
			operand2 = self.predicateJoin[2]
			operands = [operand1,operand2]
			if (operand1 in columnName1 and operand2 in columnName2 and comparisonOperator == "="):
				dfOutput = self.inputLeft.merge(self.inputRight, left_on=operand1, right_on= operand2, how="left")
			else:
				dfOutput = self.inputLeft.join(self.inputRight)
		else:
			dfOutput = self.inputLeft.join(self.inputRight)
		return dfOutput
