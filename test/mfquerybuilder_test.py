import mfquerybuilder
import phiparser

# ------ test string ------
input_string = '''
        SELECT ATTRIBUTE(S):
        cust,prod, 1_sum_quant, 2_sum_quant, 3_sum_quant
        NUMBER OF GROUPING VARIABLES(n):
        3
        GROUPING ATTRIBUTES(V):
        cust, prod
        F - VECT([F]):
        1_sum_quant, 1_avg_quant, 2_sum_quant, 3_sum_quant, 3_avg_quant
        SELECT CONDITION - VECT([σ]):
        1.state = 'NY'
        2.state = 'NJ'
        3.state = 'CT'
        '''

# ------ init setup ------
parser = phiparser.PhiParser()
parser.parse_input_string(input_string)
parser.show_dict()
builder = mfquerybuilder.MfQueryBuilder(parser.phi_dict)

#builder.mf_structure_builder()
#builder.mf_table_builder()
#builder.grouping_attribute_process()
#builder.processing_logic()
#builder.end()

builder.run()
builder.create_python_file("./output/genTest.py", builder.output)
print(builder.output)
builder.db_pool.close_all_connections()
