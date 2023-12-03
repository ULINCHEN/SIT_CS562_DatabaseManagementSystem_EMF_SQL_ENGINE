import sql
import dbconnection
import tabulate


class MfQueryBuilder:

    def __init__(self, mfStructure):

        self.db_pool = dbconnection.DatabaseConnectionPool()
        self.connection = self.db_pool.get_connection()

        self.cursor = self.connection.cursor()
        self.cursor.execute("SELECT * FROM sales")
        result = self.cursor.fetchall()
        print("------ step1 - data loaded from database ------")
        query = "SELECT column_name FROM information_schema.columns WHERE table_name = 'sales';"
        self.cursor.execute(query)
        self.columns = [column[0] for column in self.cursor.fetchall()]
        print("------ step2 - get all columns from table ------")
        print(self.columns)
        self.mfStructure = mfStructure
        # self.columns = sql.query_schema("sales")
        self.groupingValue = {}
        self.output = ""

    def grouping_attribute_builder(self):
        cacheList = []
        body = """
        """

        for item in self.mfStructure["GROUPING_ATTRIBUTES_v"]:
            entityName = item
            entityIndex = self.columns.index(entityName)
            self.groupingValue[f'{entityName}'] = {}
            template = f'''          
            distinctValue = Set()
            firstScanResult = []
            for row in data:
                if row[{entityIndex}] not in distinctValue:
                    distinctValue.add(row[{entityIndex}])
                    firstScanResult.append(row)
            '''
            cacheList.append(template)

        return body.join(cacheList)

    def grouping_variable_builder(self):
        cacheList = []
        body = """
        """

        for item in self.mfStructure["SELECT_CONDITION"]:

            condition_number = item.number
            condition_target = item.target
            condition_sign = item.sign
            condition = item.condition

            entityIndex = self.columns.index(condition_target)
            
            if condition_sign == "=":
                condition_sign = "=="

            for vect in self.mfStructure["F - VECT_f"]:
                vect_number = vect.number
                vect_aggregate = vect.aggregate
                vect_target = vect.target
                vect_value = vect.value

            template = f'''
            # Grouping variable : {condition_number} {condition_target} {condition_sign} {condition}
            for row in data:
                if row[{entityIndex}] {condition_sign} {condition}:
                    
            '''

# first, use parser to build mf structure
# then inject the dependency to query builder
# parser = new phi_parser(some_string)
# structure = parser.mf_dict
# query_builder = mf_query_builder(structure)
