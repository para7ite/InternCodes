import pandas as pd
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
        print(f"Error in exec_query: '{err}'")

def listToString(s):
    str1 = ""
    for ele in s:
        str1 += '"'+ele+'"' + ","
    return str1[:-1]

def listToStringWithoutQuote(s):
    str1 = ""
    for ele in s:
        str1 += ele + ","
    return str1[:-1]

#variables
table_name="assignment"
location=r"C:\Users\paras\Desktop\assignment.csv"
host_link="localhost"
user_name="root"
password="parasasija"
database_name="cms"
#variables

db=create_db_connection(host_link,user_name,password,database_name)
data=pd.read_csv(location)

header=data.columns.tolist()
values=data.values.tolist()

execute_query(db,f"create table {table_name}  (id int(1) NOT NULL AUTO_INCREMENT, PRIMARY KEY (id))")

for i in header:
    add_column = f"ALTER TABLE {table_name} ADD {i} VARCHAR(255)"
    execute_query(db,add_column)

for i in values:
    insert_query =f"INSERT INTO {table_name} ({listToStringWithoutQuote(header)}) VALUES ({listToString(i)})"
    execute_query(db,insert_query)