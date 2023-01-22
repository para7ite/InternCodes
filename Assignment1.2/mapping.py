import mysql.connector
from mysql.connector import Error
import pandas as pd

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

def unique(list1):
    unique_list = []
    for x in list1:
        if pd.isnull(x):
            continue
        if x not in unique_list:
            unique_list.append(x)
    return unique_list

def remove_char(s):
    s=s.replace(" ","")
    s=s.replace(".","")
    s=s.replace(")","")
    s=s.replace("/","")
    s=s.replace("(","")
    return s

def transpose(l1):
    l2 = []
    for i in range(len(l1[0])):
        row =[]
        for item in l1:
            row.append(item[i])
        l2.append(row)
    return l2

def unique(list1):
    unique_list = []
    for x in list1:
        if pd.isnull(x):
            continue
        if x not in unique_list:
            unique_list.append(x)
    return unique_list

def unique2(list1):
    unique_list = []
    for i in list1:
        if i[2] not in unique_list:
            unique_list.append(i[2])
    return unique_list

def remove_char(s):
    s=s.replace(" ","")
    s=s.replace(".","")
    s=s.replace(")","")
    s=s.replace("/","")
    s=s.replace("(","")
    return s

def transpose(l1):
    l2 = []
    for i in range(len(l1[0])):
        row =[]
        for item in l1:
            row.append(item[i])
        l2.append(row)
    return l2

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

def findOmniValues(entity,attribute,list):
    omni_values=[]
    for i in list:
        if i[0]==entity and i[1]==attribute:
            omni_values.append(i[2])
            omni_values.append(i[3])       
    return omni_values

def list_filler(result,table_name,mapping_values):
    df=pd.read_csv(dict[table_name])
    header=df.columns.tolist()
    
    for i in header:
        l2=[]
        l2=findOmniValues(table_name,i,mapping_values)
        if len(l2)!=0 and l2[1]!="nn":
            temp=[]
            temp.append(table_name)
            temp.append(i)
            temp.append(l2[0])
            temp.append(l2[1])
            temp.append(df[i].tolist())
            result.append(temp)

def omniTableToCSVandDB(table_name_omni):
    out1=[]
    out2=[]
    dict={}

    for i in result:
        if(i[2]==table_name_omni):
            out1.append(i[3])
            out2.append(i[4])
    out2_t=transpose(out2)

    for i in range(len(out2_t)):
        insert_query =f"INSERT INTO {table_name_omni} ({listToStringWithoutQuote(out1)}) VALUES ({listToString(out2_t[i])})"
        execute_query(db,insert_query)

    for i in range(len(out1)):
        dict.update({out1[i]:out2[i]})
    df2=pd.DataFrame(dict)
    df2.to_csv(r"C:\Users\paras\Desktop\outcsv\omni"+table_name_omni+".csv",index=False)
    
def transform(list,omni_attribute):
    if(omni_attribute=="Name"):
        for i in range(len(list[4])):
            list[4][i]=list[4][i]+" test"

db=create_db_connection("localhost","root","parasasija","mapping")

#files location
source_map=r"C:\Users\paras\Desktop\outcsv\modified_map.csv"
source_map_updated=r"C:\Users\paras\Desktop\outcsv\modified_map_updated.csv"
l1=r"C:\Users\paras\Desktop\outcsv\accounts.csv"
l2=r"C:\Users\paras\Desktop\outcsv\related_accounts.csv"
l3=r"C:\Users\paras\Desktop\outcsv\secondary_address.csv"
l4=r"C:\Users\paras\Desktop\outcsv\contacts.csv"
#files location


#reading source file
df=pd.read_csv(source_map)
#reading source file

#convert to list
data=df.values.tolist()
#convert to list

#omni columns correction
for i in data:
    if i[3]=="nn":
        continue
    elif i[3]=="GAP":
        i[3]=remove_char(i[1])
#omni columns correction

#creating tables
table_names=df["Entity Name Omni"].tolist()
unique_table_names=unique(table_names)
for i in unique_table_names:
    create_table(db,i)
#creating tables

#column adding
for i in data:
    if i[3]=="nn" or pd.isnull(i[3]):
        continue
    else:
        add_column(db,i[2],i[3])
#column adding

data_t=transpose(data)

#output file
dict={"Entity Name Source":data_t[0],"Attribute Name Source":data_t[1],"Entity Name Omni":data_t[2],"Attribute Name Omni":data_t[3]}
df_out = pd.DataFrame(dict)
df_out.to_csv(source_map_updated,index=False)
#output file

#reading updated map file
df=pd.read_csv(source_map_updated)
mapping_values=df.values.tolist()
#reading updated map file

#writing omni tables to csv and db
dict={"Accounts":l1,"Related Accounts":l2,"Secondary Address":l3,"Contacts":l4}
result=[]
unique_omni_table=[]

for i in dict:
    list_filler(result,i,mapping_values)

transform(result[1],"Name")
print(result[1])
unique_omni_table=unique2(result)

for i in unique_omni_table:
   omniTableToCSVandDB(i)

#writing omni tables to csv and db