
import dbconnection
import tabulate
import pandas as pd


class MfQueryBuilder:

    def __init__(self, mfStructure):
        print("------ MF query builder initializing ------")
        self.db_pool = dbconnection.DatabaseConnectionPool()
        self.connection = self.db_pool.get_connection()

        self.cursor = self.connection.cursor()
        self.cursor.execute("SELECT * FROM sales")
        result = self.cursor.fetchall()
        #print("------ step1 - data loaded from database ------")
        #query = "SELECT column_name FROM information_schema.columns WHERE table_name = 'sales';"
        #self.cursor.execute(query)
        self.columns = ['cust', 'prod', 'day', 'month', 'year', 'state', 'quant', 'date']
        #print("------ step2 - get all columns from table ------")
        # print(self.columns)
        self.groupingAttributeIndex = None
        self.mfStructure = mfStructure
        # self.columns = sql.query_schema("sales")
        self.groupingValue = {}
        self.groupingAttributeIndexStack = []

        for item in self.mfStructure["GROUPING_ATTRIBUTES_v"]:
            self.groupingAttributeIndexStack.append(self.columns.index(item))


        self.output = f"""

# ------ Fetching Raw Data From Database ------
import dbconnection
from tabulate import tabulate


db_pool = dbconnection.DatabaseConnectionPool()
connection = db_pool.get_connection()

cursor = connection.cursor()
cursor.execute("SELECT * FROM sales")


# ------ table & columns ------ \n
table = cursor.fetchall()
columns = {self.columns}
        """

    def mf_structure_builder(self):
        print("------ BUILDING MF STRUCTURE ------")
        # build string of grouping attribute
        groupingAttr = ""
        for item in self.mfStructure["GROUPING_ATTRIBUTES_v"]:
            #print("item:" + item)
            groupingAttr += "\"" + item + "\":[],"
        groupingAttr = groupingAttr[:-1]
        groupingAttr = "{" + groupingAttr + "}"

        # ----------------F - VECT_f---------------------

        # build string of aggregate function list
        aggregateAttr = ""
        # print(self.mfStructure["F - VECT_f"].values())
        for value in self.mfStructure["F - VECT_f"].values():
            for v in value:
                stringBuilder = ""
                stringA = "\n          \"number\":" + v.number + ",\n"
                stringB = "          \"aggregate\":" + "\"" + v.agg + "\"" + ",\n"
                stringC = "          \"target\": \"" + v.target + "\",\n"
                stringD = "          \"value\": " + str(v.value) + ",\n"
                stringBuilder = stringBuilder + stringA + stringB + stringC + stringD
                stringBuilder = "     {" + stringBuilder + "     }"
                aggregateAttr = aggregateAttr + stringBuilder + ",\n"

        #    value = value[0]
        #    stringBuilder = ""
        #    stringA = "\n          \"number\":" + value.number + ",\n"
        #    stringB = "          \"aggregate\":" + "\"" + value.agg + "\"" + ",\n"
        #    stringC = "          \"target\": \"" + value.target + "\",\n"
        #    stringD = "          \"value\": " + str(value.value) + ",\n"
        #    stringBuilder = stringBuilder + stringA + stringB + stringC + stringD
        #    stringBuilder = "     {" + stringBuilder + "     }"
        #    aggregateAttr = aggregateAttr + stringBuilder + ",\n"

        aggregateAttr = aggregateAttr.rstrip(',\n')
        aggregateAttr = "[\n" + aggregateAttr + "\n     ]"

        # ----------------SELECT_CONDITION----------------

        selectAttr = """
        """
        for v in self.mfStructure["SELECT_CONDITION"].values():
            for item in v:
                #print(item)
                number = item.number
                target = item.target
                sign = item.sign
                condition = item.condition
                temp = f"""
{{
    "number":{number},
    "target":"{target}",
    "sign":"{sign}",
    "condition":"{condition}",
}},
"""
                selectAttr += temp


        # -------------------------------------------

        tableColTitle = []
        for item in self.mfStructure["GROUPING_ATTRIBUTES_v"]:
            tableColTitle.append(item)

        for value in self.mfStructure["F - VECT_f"].values():
            for v in value:
                stringBuilder = "" + v.number + "_" + v.agg + "_" + v.target
                tableColTitle.append(stringBuilder)
            # value = value[0]
            # stringBuilder = "" + value.number + "_" + value.agg + "_" + value.target
            # tableColTitle.append(stringBuilder)

        # -------------------------------------------

        template = f'''
mfObj = {{
    "groupingAttribute":{groupingAttr},
    "groupingResultDict":{{}},
    "groupingResult":[{tableColTitle}],
    "numOfGroupingAttributes":{len(self.mfStructure["GROUPING_ATTRIBUTES_v"])},
    "numOfGroupingVariables": {self.mfStructure["NUMBER_OF_GROUPING_VARIABLES_n"]},
    "aggregateList":{
        aggregateAttr
    },
    "selectAttributes":[{
        selectAttr
    }]
}}
'''
        # print("------ MF Structure ------")
        # print(template)
        self.output += "\n# ------ MF Structure ------ \n"
        self.output += template
        return self.output

    def mf_table_builder(self):
        print("------ Building MF Table ------")
        table_entity = []

        temp = ""

        for item in self.mfStructure["GROUPING_ATTRIBUTES_v"]:
            table_entity.append("\"" + item + "\"")

        for key, value in self.mfStructure["F - VECT_f"].items():
           # print(key)
           # print(value[0])
            for item in value:
                title = "\"" + item.number + "_" + item.agg + "_" + item.target + "\""
                #print("title: " + title)
                table_entity.append(title)

        for element in table_entity:
            temp += element
            temp += ","

        temp = "[" + temp[:-1] + "]"

        res = f"""

rawTableEntity = {temp}

mfTable = pd.DataFrame(columns = table_entity)

        """

        self.mf_table = pd.DataFrame(columns = table_entity)
        #print("MF_Table is Build:\n")
        #print(self.mf_table)
        self.output += res
    def grouping_attribute_process(self):
        cacheList = []
        body = """
        """

        for item in self.mfStructure["GROUPING_ATTRIBUTES_v"]:
            entityName = item
            entityIndex = self.columns.index(entityName)
            self.groupingAttributeIndex = entityIndex
            template = f'''
res = set()
for row in table:
    res.add(row[{entityIndex}])
mfObj["groupingAttribute"]["{entityName}"] = list(res)
            '''
            #cacheList.append(template)
            body += template
        #body.join(cacheList)
        self.output += "\n# ------ populate mf-struct with distinct values of grouping attribute ------"
        self.output += body

        return self.output

    def processing_logic(self):

        head = "\nfor row in table:"

        for k, v in self.mfStructure["SELECT_CONDITION"].items():
            for item in v:
                n = item.number
                t = item.target
                s = item.sign
                c = item.condition

                if s == "=":
                    s = "=="

                i = self.columns.index(t)


                res = f"""    
    if row[{i}] {s} "{c}":
    
        
        
        currentNumber = {n}
        # if we found new row value, build the key
        key = ""
        for k in mfObj["groupingAttribute"].keys():
                key += row[columns.index(k)]
                key += "_"            
                # search if this key in result and update each agg result
        key = key[:-1]
        if key not in mfObj["groupingResultDict"]:
                mfObj["groupingResultDict"][key] = {{}}
                
                
        for item in mfObj["aggregateList"]:
                                
                        number = item["number"]
                        aggregate = item["aggregate"]
                        target = item["target"]
                        value = item["value"]
                        
                        if number != currentNumber:
                            continue
                                           
                        subKey = str(number) + "_" + aggregate + "_" + target
                        targetIndex = columns.index(target)
                        
                        if subKey not in mfObj["groupingResultDict"][key]:
                            if aggregate == "min":
                                mfObj["groupingResultDict"][key][subKey] = float('inf')
                            elif aggregate == "max":
                                mfObj["groupingResultDict"][key][subKey] = float('-inf')
                            else:
                                mfObj["groupingResultDict"][key][subKey] = 0     
                        
                        match aggregate:
                            case "sum":
                                mfObj["groupingResultDict"][key][subKey] += row[targetIndex]
                                                
                            case "count":
                                mfObj["groupingResultDict"][key][subKey] += 1
                                                
                            case "min":
                                mfObj["groupingResultDict"][key][subKey] = min(row[targetIndex], mfObj["groupingResultDict"][key][subKey])
                                
                            case "max":
                                mfObj["groupingResultDict"][key][subKey] = max(row[targetIndex], mfObj["groupingResultDict"][key][subKey])
                                            
                            case "avg":
                                
                                if "sum" not in mfObj["groupingResultDict"][key]:
                                    mfObj["groupingResultDict"][key]["sum"] = 0
                                if "count" not in mfObj["groupingResultDict"][key]:
                                    mfObj["groupingResultDict"][key]["count"] = 0
                                
                                mfObj["groupingResultDict"][key]["sum"] += row[targetIndex]
                                mfObj["groupingResultDict"][key]["count"] += 1
                                mfObj["groupingResultDict"][key][subKey] = mfObj["groupingResultDict"][key]["sum"] / mfObj["groupingResultDict"][key]["count"]
"""
                head += res
        self.output += head

    def end(self):
        print("------ Processing Logic Build Finished ------")
        self.output += "\ndb_pool.close_all_connections()"
        #self.output += "\nprint(mfObj)"


        self.output += """
for k, v in mfObj["groupingResultDict"].items():
    temp = []
    keyList = k.split("_")
    for i in keyList:
        temp.append(i)
    for key, value in v.items():
        if key == "sum" or key == "count": continue
        temp.insert(mfObj["groupingResult"][0].index(key), value)
    mfObj["groupingResult"].append(temp)
            
t = tabulate(mfObj["groupingResult"])
print(t)
        """

    def create_python_file(self, file_path):
        try:
            with open(file_path, 'w') as file:
                file.write(self.output)
            print(f"Python file created at --> {file_path}")
        except Exception as e:
            print(f"Error when creating python file：{e}")

    def run(self):
        self.mf_structure_builder()
        self.grouping_attribute_process()
        self.processing_logic()
        self.end()

# first, use parser to build mf structure
# then inject the parser instance to query builder
# parser = new phi_parser(some_string)
# structure = parser.mf_dict
# query_builder = mf_query_builder(structure)
