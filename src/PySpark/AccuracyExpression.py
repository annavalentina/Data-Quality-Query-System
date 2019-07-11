import re 
import time
from datetime import datetime
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
################################################################################
# This is a support function to the Accuracy operator. This class uses recursion 
# to process complex AccuracyExpression expressions. 
#################################################################################
class AccuracyExpression():

	def __init__(self,inputInput,inputOperandLeft,inputOperator,inputOperandRight):
		self.input = inputInput
		self.operandLeft = inputOperandLeft
		self.operator = inputOperator
		self.operandRight = inputOperandRight
	
	def apply(self):
    # The apply function of this class uses recursion to process complex AccuracyExpression expressions. 
	# The expressions are of the following type: operandLeft operator operandRight
	# The operands can be either dataframes or new expressions	
	
		def addColumns(df): # Add the values of two columns
			dfSum=df.withColumn('result', sum(df[col] for col in [op_left_dt,op_right_dt]))
			return dfSum
			
		def findEquality(x1,x2): # Find if the values of two columns are equal
			if x1==x2:
				return 100
			else:
				return 0
		func_udf_eq=udf(findEquality,IntegerType())

		def findBiggerThan(x1,x2): # Find if the value of one column is bigger than the value of another one
			if x1>x2:
				return 100
			else:
				return 0
		func_udf_bigger=udf(findBiggerThan,IntegerType())
                

		op_left_dt = self.operandLeft
		if (isinstance(self.operandRight, AccuracyExpression) == True): # If operandRight is an expression
			op_right_accExpr = self.operandRight
                        
		elif (isinstance(self.operandRight, set) == True): # If operandRight is a dataframe
			op_right_dt = self.operandRight
                        
		else:
			op_right_dt = self.operandRight
                        

                        
		if (isinstance(self.operandRight, AccuracyExpression) == True): # If operandRight is an expression
			op_right_dt,self.input = self.operandRight.apply() # Recursion
                        
			if (self.operator == "="):
				result=self.input.withColumn('accuracy', func_udf_eq(self.input[op_left_dt],self.input[op_right_dt]))
				return result
			elif (self.operator == ">"):
				result=self.input.withColumn('accuracy', func_udf_bigger(self.input[op_left_dt],self.input[op_right_dt]))
				return result
			elif (self.operator == "+"):
				result=addColumns(self.input)
				return "result",result

		else:
			if (self.operator == "="):
				result=self.input.withColumn('accuracy', func_udf_eq(self.input[op_left_dt],self.input[op_right_dt]))
				return result
			elif (self.operator == ">"):
				result=self.input.withColumn('accuracy', func_udf_bigger(self.input[op_left_dt],self.input[op_right_dt]))
				return result
			elif (self.operator == "+"):
				result=addColumns(self.input)
				return "result",result


# -------------- Cases to be added in the Future ------------------------
#                elif (self.operator == "!="):
#                        return op_left_dt.ne(op_right_dt)
#
#                elif (self.operator == ">"):
#                        return op_left_dt.gt(op_right_dt)
#                        
#                elif (self.operator == "<"):
#                        return op_left_dt.lt(op_right_dt)
#
#                elif (self.operator == ">="):
#                        return op_left_dt.ge(op_right_dt)
#
#                elif (self.operator == "<="):
#                        return op_left_dt.le(op_right_dt)
#
#                elif (self.operator == "-"):
#                        return (op_left_dt - op_right_dt) 
#
#                elif (self.operator == "*"):
#                        return (op_left_dt * op_right_dt)
#
#                elif (self.operator == "/"):
#                        return (op_left_dt / op_right_dt)
#
#                else:
#                        return -10000.00
#---------------------------------------------------------------------  
