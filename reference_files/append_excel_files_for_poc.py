import pandas as pd
import xlwings as xw
import os

import pandas as pd

dir = r'C:\[공부]Postgre, Neo4J, python, javascript\Neo4J\Quiz\Use Case2\data'
os.chdir(dir)
file_check= ".xlsx"

""" 방법1 """
# xlxs_list = [file for file in os.listdir(dir) if file.endswith(file_check)]

df_all = pd.DataFrame()
# """ 방법2 """
for file in os.listdir(dir) : 
    if file.endswith(file_check) :
        print(file)
        df = pd.read_excel(file)
        df_all = df_all.append(df)
        
xw.view(df_all)