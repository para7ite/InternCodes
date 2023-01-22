
from mapping_func import *
from staging import *

result=stagingList()
for i in range(len(result)):
    if result[i][2]=="Account" and result[i][3]=="indskr_externalid":
        print(result[i][4])
        
def transformList():
    return result