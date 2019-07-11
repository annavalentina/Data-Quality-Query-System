################################################################################
# Filters a dynamic generic array and creates a new one based on the result 
# obtained from the Predicate class.
#################################################################################
class Select():

	def __init__(self, inputInput, inputPredicate):
		self.input = inputInput
		self.predicate = inputPredicate
		self.output = []
		
	def execute(self):
		if (self.predicate != None):
			dfOutput = self.predicate.apply(self.input)
			return dfOutput
		else:
			return dfInput
