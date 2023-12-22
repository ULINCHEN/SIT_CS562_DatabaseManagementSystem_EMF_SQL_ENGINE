# database.py

import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv
import os

load_dotenv('program.env')


class DatabaseConnectionPool:
    def __init__(self):

        self.DB_HOST = os.getenv('DBHOST')
        self.DB_PORT = os.getenv('DBPORT')
        self.DB_NAME = os.getenv('DBNAME')
        self.DB_USER = os.getenv('USER')
        self.DB_PASSWORD = os.getenv('PASSWORD')

        self.connection_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            host=self.DB_HOST,
            port=self.DB_PORT,
            database=self.DB_NAME,
            user=self.DB_USER,
            password=self.DB_PASSWORD
        )

    def get_connection(self):

        connection = self.connection_pool.getconn()
        return connection

    def release_connection(self, connection):

        self.connection_pool.putconn(connection)

    def close_all_connections(self):

        self.connection_pool.closeall()

"""
# use example

from database import DatabaseConnectionPool
import tabulate

# create connection pool
db_pool = DatabaseConnectionPool()

# get connection
connection = db_pool.get_connection()

# execute query operation
cursor = connection.cursor()
cursor.execute("SELECT * FROM sales")
result = cursor.fetchall()
print(tabulate.tabulate(result, headers="keys", tablefmt="simple"))

# release connection
db_pool.release_connection(connection)

# close connection
# db_pool.close_all_connections()
"""
