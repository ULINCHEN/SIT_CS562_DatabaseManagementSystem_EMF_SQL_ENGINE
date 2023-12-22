

# ------ Fetching Raw Data From Database ------
import dbconnection
from tabulate import tabulate


db_pool = dbconnection.DatabaseConnectionPool()
connection = db_pool.get_connection()

cursor = connection.cursor()
cursor.execute("SELECT * FROM sales")


# ------ table & columns ------ 

table = cursor.fetchall()
columns = ['cust', 'prod', 'day', 'month', 'year', 'state', 'quant', 'date']
        
# ------ MF Structure ------ 

mfObj = {
    "groupingAttribute":{"cust":[]},
    "groupingResultDict":{},
    "groupingResult":[['cust', '1_sum_quant', '2_sum_quant']],
    "numOfGroupingAttributes":1,
    "numOfGroupingVariables": 2,
    "aggregateList":[
     {
          "number":1,
          "aggregate":"sum",
          "target": "quant",
          "value": 0,
     },
     {
          "number":2,
          "aggregate":"sum",
          "target": "quant",
          "value": 0,
     }
     ],
    "selectAttributes":[
        
{
    "number":1,
    "target":"state",
    "sign":"=",
    "condition":"NY",
},

{
    "number":2,
    "target":"state",
    "sign":"=",
    "condition":"CT",
},
]
}

# ------ populate mf-struct with distinct values of grouping attribute ------
        
res = set()
for row in table:
    res.add(row[0])
mfObj["groupingAttribute"]["cust"] = list(res)
            
for row in table:    
    if row[5] == "NY":
    
        
        
        currentNumber = 1
        # if we found new row value, build the key
        key = ""
        for k in mfObj["groupingAttribute"].keys():
                key += row[columns.index(k)]
                key += "_"            
                # search if this key in result and update each agg result
        key = key[:-1]
        if key not in mfObj["groupingResultDict"]:
                mfObj["groupingResultDict"][key] = {}
                
                
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
    
    if row[5] == "CT":
    
        
        
        currentNumber = 2
        # if we found new row value, build the key
        key = ""
        for k in mfObj["groupingAttribute"].keys():
                key += row[columns.index(k)]
                key += "_"            
                # search if this key in result and update each agg result
        key = key[:-1]
        if key not in mfObj["groupingResultDict"]:
                mfObj["groupingResultDict"][key] = {}
                
                
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

db_pool.close_all_connections()
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
        