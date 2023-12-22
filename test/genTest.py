

# ------ Fetching Raw Data From Database ------
import dbconnection
from tabulate import tabulate


db_pool = dbconnection.DatabaseConnectionPool()
connection = db_pool.get_connection()

cursor = connection.cursor()
cursor.execute("SELECT * FROM sales")


# ------ table columns ------ 

table = cursor.fetchall()
columns = ['cust', 'prod', 'day', 'month', 'year', 'state', 'quant', 'date']
        
# ------ MF Structure ------ 

mfObj = {
    "groupingAttribute":{"cust":[],"prod":[]},
    "groupingResultDict":{},
    "groupingResult":[['cust', 'prod', '1_sum_quant', '2_sum_quant', '3_sum_quant']],
    "numOfGroupingAttributes":2,
    "numOfGroupingVariables": 3,
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
     },
     {
          "number":3,
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
    "condition":"NJ",
},

{
    "number":3,
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
            
res = set()
for row in table:
    res.add(row[1])
mfObj["groupingAttribute"]["prod"] = list(res)
            
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
                                mfObj["groupingResultDict"][key]["sum"] += row[targetIndex]
                                mfObj["groupingResultDict"][key]["count"] += 1
                                mfObj["groupingResultDict"][key][subKey] = mfObj["groupingResultDict"][key]["sum"] / mfObj["groupingResultDict"][key]["count"]
    
    if row[5] == "NJ":
    
        
        
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
                                mfObj["groupingResultDict"][key]["sum"] += row[targetIndex]
                                mfObj["groupingResultDict"][key]["count"] += 1
                                mfObj["groupingResultDict"][key][subKey] = mfObj["groupingResultDict"][key]["sum"] / mfObj["groupingResultDict"][key]["count"]
    
    if row[5] == "CT":
    
        
        
        currentNumber = 3
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
                                mfObj["groupingResultDict"][key]["sum"] += row[targetIndex]
                                mfObj["groupingResultDict"][key]["count"] += 1
                                mfObj["groupingResultDict"][key][subKey] = mfObj["groupingResultDict"][key]["sum"] / mfObj["groupingResultDict"][key]["count"]

db_pool.close_all_connections()
print(mfObj)
for k, v in mfObj["groupingResultDict"].items():
    temp = []
    keyList = k.split("_")
    for i in keyList:
        temp.append(i)
    for value in v.values():
        temp.append(value)
    mfObj["groupingResult"].append(temp)
            
t = tabulate(mfObj["groupingResult"])
print(t)
        