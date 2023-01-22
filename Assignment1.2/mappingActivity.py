import pandas as pd
from mapping_func import *

#files location
knowledge_base=r"C:\Users\paras\Desktop\outcsv\knowledge_base.csv"
knowledge_base_updated=r"C:\Users\paras\Desktop\outcsv\knowledge_base_updated.csv"
#files location


df=pd.read_csv(knowledge_base) #reading source file
data=df.values.tolist() #convert to list

updated_data=[] #omni columns correction
for i in range(len(data)): 
    if pd.isnull(data[i][3]) or data[i][3]=="GAP":
        continue
    else:
        updated_data.append(data[i])

data_t=transpose(data) #saving updated knowledge base
dict={"Entity Name Source":data_t[0],"Attribute Name Source":data_t[1],"Entity Name Omni":data_t[2],"Attribute Name Omni":data_t[3]}
df_out = pd.DataFrame(dict)
df_out.to_csv(knowledge_base_updated,index=False)