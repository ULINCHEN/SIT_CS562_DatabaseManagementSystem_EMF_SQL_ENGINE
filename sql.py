import dbconnection
import INPUT_STRING
from tabulate import tabulate

inputString = INPUT_STRING.INPUT_STRING()

# create connection pool
db_pool = dbconnection.DatabaseConnectionPool()

# get connection
connection = db_pool.get_connection()

# execute query operation
cursor = connection.cursor()
cursor.execute(inputString.SQL_ONE)
result = cursor.fetchall()

print(tabulate(result, headers="keys", tablefmt="simple"))

# release connection
db_pool.close_all_connections()

# close connection
# db_pool.close_all_connections()