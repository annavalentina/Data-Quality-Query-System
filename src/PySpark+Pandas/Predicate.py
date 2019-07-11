################################################################################
# Evaluates a given predicate and returns a boolean value depending on its 
# compliment or not, according to the operator and the evaluated values within it.
#################################################################################
class Predicate(object):
	def __init__(self,inputOperand1,inputComparisonOperator,inputOperand2):
		self.operand1 = inputOperand1
		self.comparisonOperator = inputComparisonOperator
		self.operand2 = inputOperand2
	
	def apply(self,dfInput):
		operand1_true = 0
		nameAttributes = dfInput.columns
		typeAttributes = dfInput.dtypes
		if (self.operand1 in nameAttributes):
			positionOfAttribute = nameAttributes.get_loc(self.operand1)
			attributeType = str(typeAttributes[positionOfAttribute])
			operand1_true = 1
			
		if (operand1_true != 0):
			
			if (attributeType != "object"):
				self.operand1 = str(self.operand1)
				op_2 = str(self.operand2)             
				if (self.comparisonOperator == "<"):
					filterClause = ""+self.operand1+"<"+op_2+""
					print(filterClause)
					dfOutput = dfInput.query(filterClause) 
				elif(self.comparisonOperator == "<="):
					filterClause = ""+self.operand1+"<="+op_2+""
					dfOutput = dfInput.query(filterClause)
				elif(self.comparisonOperator == "="):
					filterClause = ""+self.operand1+"="+op_2+""
					dfOutput = dfInput.query(filterClause)               
				elif(self.comparisonOperator == ">"):
					filterClause = ""+self.operand1+">"+op_2+""
					dfOutput = dfInput.query(filterClause)
				elif(self.comparisonOperator == ">="):
					filterClause = ""+self.operand1+">="+op_2+""
					dfOutput = dfInput.query(filterClause)
				elif(self.comparisonOperator == "<>"):
					filterClause = ""+self.operand1+"!="+op_2+""
					dfOutput = dfInput.query(filterClause)
			
			elif (attributeType == "object"):
				op_2 = str(self.operand2)
				if(self.comparisonOperator == "="):
					filterClause = ""+self.operand1+"=='"+op_2+"'"
					dfOutput = dfInput.query(filterClause)               
				elif(self.comparisonOperator == "<>"):
					filterClause = ""+self.operand1+"!='"+op_2+"'"
					dfOutput = dfInput.query(filterClause) 
			return dfOutput
		else:
			return dfInput
