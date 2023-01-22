import mysql.connector
from mysql.connector import Error

def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(host=host_name,user=user_name,passwd=user_password,database=db_name)
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error creating db: '{err}'")

    return connection

def execute_query(connection, query):
    cursor = connection.cursor(buffered=True)
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error in exec_query: '{err} in {query}'")

def create_table(connection, table_name):
    create_table_query = f"CREATE TABLE {table_name} (id INT(1) NOT NULL AUTO_INCREMENT, PRIMARY KEY (id))"
    execute_query(connection, create_table_query)

def add_column(connection, table_name, column_name):
    add_column = f"ALTER TABLE {table_name} ADD {column_name} VARCHAR(255)"
    execute_query(connection,add_column)