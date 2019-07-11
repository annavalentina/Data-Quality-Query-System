import re 
import time
from datetime import datetime
import numpy as np
import pandas as pd
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
    # The apply function of this class uses recursion to process complex AccuracyExpression expressions
	# The expressions are of the following type: operandLeft operator operandRight
	# The operands can be either dataframes or new expressions

                
		op_left_dt = self.input[self.operandLeft]
                
		if (isinstance(self.operandRight, AccuracyExpression) == True): # If operandRight is an expression
			op_right_accExpr = self.operandRight
                        
		elif (isinstance(self.operandRight, set) == True): # If operandRight is a dataframe
			op_right_dt = self.operandRight
		else:
			op_right_dt = self.input[self.operandRight]
                        
	
                        
		if (isinstance(self.operandRight, AccuracyExpression) == True): # If operandRight is an expression
			op_right_dt = self.operandRight.apply() # Recursion
           
			if (self.operator == "="):
				return op_left_dt.eq(op_right_dt)
			elif (self.operator == ">"):
				return op_left_dt.gt(op_right_dt)
			elif (self.operator == "+"):
				return (op_left_dt + op_right_dt)

		else: # If operandRight is a dataframe
			if (self.operator == "="):
				return op_left_dt.eq(op_right_dt)
			elif (self.operator == ">"):
				return op_left_dt.gt(op_right_dt)
			elif (self.operator == "+"):
				return (op_left_dt + op_right_dt)
			elif (self.operator == "in"):
				return op_left_dt.isin(op_right_dt)


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
