import os
import psycopg2  # Postgre SQL adapter
import psycopg2.extras  # extra module of psycopg2
import tabulate  # for convert data to table format
from dotenv import load_dotenv  # for load settings from env file, but where is it?


def query_and_print():
    """
    Used for testing standard queries in SQL.
    """
    load_dotenv('program.env')

    # load database connection param
    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    # connect to database
    conn = psycopg2.connect("dbname=" + dbname + " user=" + user + " password=" + password,
                            cursor_factory=psycopg2.extras.DictCursor)

    cur = conn.cursor()

    cur.execute("SELECT * FROM sales WHERE quant > 100")  # do SQL query
    print("finished test")
    return tabulate.tabulate(cur.fetchall(),
                             headers="keys", tablefmt="simple")


def query_and_return():
    load_dotenv('program.env')

    # load database connection param
    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    # connect to database
    conn = psycopg2.connect("dbname=" + dbname + " user=" + user + " password=" + password,
                            cursor_factory=psycopg2.extras.DictCursor)

    cur = conn.cursor()

    cur.execute("SELECT * FROM sales WHERE quant > 100")  # do SQL query
    return cur.fetchall()


def query_schema(tableName):
    load_dotenv('program.env')

    # load database connection param
    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    # connect to database
    conn = psycopg2.connect("dbname=" + dbname + " user=" + user + " password=" + password,
                            cursor_factory=psycopg2.extras.DictCursor)

    cur = conn.cursor()
    query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{tableName}';"
    cur.execute(query)
    columns = [column[0] for column in cur.fetchall()]
    print(columns)
    return columns


def main():
    res = query_schema("sales")
    query_and_return()

if "__main__" == __name__:
    main()
