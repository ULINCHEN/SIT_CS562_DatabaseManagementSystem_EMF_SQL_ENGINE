class INPUT_STRING:

    def __init__(self):

        self.ESQL_ONE = """
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
        """

        self.ESQL_TWO = '''
        SELECT ATTRIBUTE(S):
        cust, 1_sum_quant, 2_sum_quant
        NUMBER OF GROUPING VARIABLES(n):
        2
        GROUPING ATTRIBUTES(V):
        cust
        F - VECT([F]):
        1_sum_quant, 2_sum_quant
        SELECT CONDITION - VECT([σ]):
        1.state = 'NY'
        2.state = 'CT'
        '''

        self.ESQL_THREE = '''
        SELECT ATTRIBUTE(S):
        cust, 1_avg_quant, 2_max_quant, 2_min_quant
        NUMBER OF GROUPING VARIABLES(n):
        2
        GROUPING ATTRIBUTES(V):
        cust
        F - VECT([F]):
        1_avg_quant, 2_max_quant, 2_min_quant
        SELECT CONDITION - VECT([σ]):
        1.state = 'NY'
        2.state = 'CT'
        '''

        self.SQL_ONE = """
        SELECT x.cust, 
               SUM(x.quant) AS ny_prod_sum,
               (SELECT SUM(quant) FROM sales WHERE cust = x.cust AND state = 'CT') AS ct_prod_sum
        FROM sales x
        WHERE x.state = 'NY'
        GROUP BY x.cust;
        """