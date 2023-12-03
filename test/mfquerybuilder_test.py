import mfquerybuilder
import phiparser

# ------ test string ------
input_string = '''
        SELECT ATTRIBUTE(S):
        cust
        NUMBER OF GROUPING VARIABLES(n):
        1
        GROUPING ATTRIBUTES(V):
        cust, day, quant
        F - VECT([F]):
        1_sum_quant
        SELECT CONDITION - VECT([σ]):
        1.state = 'NY'
        '''

# ------ init setup ------
parser = phiparser.PhiParser()
parser.parse_input_string(input_string)

builder = mfquerybuilder.MfQueryBuilder(parser.phi_dict)
res = builder.grouping_attribute_builder()
print(res)
