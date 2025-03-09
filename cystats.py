import sys
import os
import re
from functools import reduce
import pandas as pd
from collections import defaultdict
import hashlib
from itertools import groupby
# import multiprocessing as mp
# from multiprocessing import Pool

class CyStats:
    def __init__(self, file_list, download=None, parallel=False, concurrency=2, transaction_to_csv=False, log_type='INFO'):
        self.file_list=file_list
        if download != None:
            self.download=download
        else:
            self.download='\\'.join(file_list[0].split('\\')[:-1])
        self.parallel=parallel
        self.concurrency=concurrency
        self.transaction_to_csv = transaction_to_csv
        self.log_type=log_type

        # hashKey : 0:query, 1:totalCount,2:totalElapsedTime,3:elasedTime(avg),4:elasedTime(min),5:elasedTime(max),6: total_memory, 7:memory(avg),8:memory(min),9:memory(max),
        # 10:dateTime(mm dd hh:mm:ss.z),11: db, 12:pattern, 13:high_pattern

        self.condition = defaultdict(list)
        self.add_filter('transaction_id','!=','-1')
        self.add_filter('type','!=','ERROR')
        self.col_dict = dict([('date','x[0]') ,('query_id','x[1]') ,('transaction_id','x[2]') ,('elapsed_time','x[3]') ,('planning_time','x[4]') ,('waiting_time','x[5]') ,('memory_usage','x[6]')      ,('page_hits','x[7]')
      ,('page_faults','x[8]')      ,('session','x[9]')    ,('query','x[10]')    ,('parameters','x[11]')    ,('action','x[12]')    ,('etc','x[13]')    ,('db','x[14]')    ,('type','x[15]')         ])
    
    
    def excute(self, file_list=None):
        if file_list == None:
            file_list = self.file_list
        query_log = defaultdict(lambda :[set(),0,0,0,0,0,0,0,0,0,'00-00 00:00:00.000',set(),'',''])
        
        for i in self.condition.keys():
            for j in self.condition[i]:
                print("실행 필터링 :  x['"+i+"']"+ j[0]+"str('"+j[1]+"')",end='')
            
        print("-------------------")
                
        for file in file_list:
            log_list = self.parsing(file)
            log_list =  self.regex(log_list)

            if self.transaction_to_csv:
                self.to_csv(log_list, self.download, file)

            log_list = self.filtering(log_list, self.condition)
            self.groupbyAndCalculate(log_list, query_log)
        test_df = pd.DataFrame(query_log.values(),columns =['query','count','total_time(ms)','avg_time(ms)','min_time(ms)','max_time(ms)','total_memory(B)','avg_memory(B)','min_memory(B)','max_memory(B)','last_log_time','db','hash','hash_'])
        test_df.to_csv(self.download+'\\export.csv',encoding="utf-8",index=False)
        print('Done!')


    def groupbyAndCalculate(self, log_list, query_log, log_type='INFO'):
        group = groupby(log_list, lambda x: (x[14],x[2],x[1]))
        groupby_dict = defaultdict(list)
        for i,j in group:
            for z in j:
                groupby_dict[i].append(z) 
                
        groupby_list = list()
        if log_type != 'INFO':
            for value in groupby_dict.values():
                                    #[0:'date',    1:'query_id',  2:'transaction_id',3:'elapsed_time(ms)',4:'planning_time(ms)',    5:'waiting_time(ms)',      6:'memory_usage(Byte)',  7:'page_hits',          8:'page_faults',        9:'session',    10:'query',  11:'parameters',12:'action',13:'etc',    14:'db',     15:'log_type']
                groupby_list.append([value[1][0],  value[0][1],  value[0][2],  value[0][3]+value[1][3],  value[0][4]+value[1][4],  value[0][5]+value[1][5],  value[0][6]+value[1][6],  value[0][7]+value[1][7],  value[0][8]+value[1][8], value[0][9], value[0][10], value[0][11], value[1][12], value[1][13], value[1][14], value[0][13]])
        else:
            for value in groupby_dict.values():
                groupby_list.extend(value)

        for i in groupby_list:
            hashKey = hashlib.sha256(i[10].encode()).hexdigest()
            query_log[hashKey][0].add(i[10]) #query
            query_log[hashKey][1] += 1 #total count
            query_log[hashKey][2] += int(i[3])
            query_log[hashKey][3] = int((query_log[hashKey][2])/query_log[hashKey][1]) if  query_log[hashKey][2] != 0 else 0 #avg Elapsed Time
            query_log[hashKey][4] = min([query_log[hashKey][4],i[3]]) if query_log[hashKey][4] != 0 else i[3] #min Elapsed Time
            query_log[hashKey][5] = max([query_log[hashKey][5],i[3]]) #max Elapsed Time
            query_log[hashKey][6] += int(i[6])
            query_log[hashKey][7] = int(query_log[hashKey][6]/query_log[hashKey][1]) if query_log[hashKey][6] != 0 else 0 #avg memory
            query_log[hashKey][8] = min([query_log[hashKey][8],i[6]]) if query_log[hashKey][8] != 0 else i[6] #min memory
            query_log[hashKey][9] = max([query_log[hashKey][9],i[6]]) #max memory
            query_log[hashKey][10] = max([query_log[hashKey][10],i[0]]) #last time
            query_log[hashKey][11].add(i[14]) #db
            query_log[hashKey][12] = hashKey
#         return query_log
            
    def parsing(self, file :str, encoding='utf-8'):
        f = open(file,"r",encoding=encoding)
        log = f.read()
        f.close()

        log_split = re.split(r'\n(?=\d\d\d\d-\d\d-\d\d\s\d\d:\d\d:\d\d\.\d\d\d\+)', log)
        print(file.split('\\')[-1], " 파싱 로우 수: " + str(len(log_split)), end='')

        log_split = [x for x in log_split if not(bool(re.search('username: null',x)))]
        temp_01 = [re.split('- neo4j -',x,maxsplit=1) for x in log_split]
        temp_01 = [ x if len(x)==2 else re.split(r'-  -',x[0],maxsplit=1) for x in temp_01 ]
        for j,i in enumerate(temp_01):
            assert len(i) == 2, file+'에 '+str(j)+'번째에서 parsing error'+', '+re.findall(r'(transaction id:.*?) -',i[0])[0]
        print(" Logs : " +str(len(temp_01)), end='')
        print("-------------------")

        return temp_01
    
    def regex(self, log_list :list):
        split_list = []    
        for x in log_list:
            try:
                split_list.append([re.findall("\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\.\d{3}\+\d{4}",x[0])[0] ,                             # 0: date
                                   int(re.findall(r"id:(\d*)\s- transaction",x[0])[0]) if re.search(r"id:(\d*)\s- transaction",x[0]) else '_',                      # 1: query
                                   int(re.findall(r"transaction id:(-?\d*)\s-",x[0])[0]),                  # 2: transaction_id
                                   int(re.findall(r"\s(\d*) ms:",x[0])[0]),                                  # 3: elapsed_time(ms)
                                   int(re.findall(r"planning: (\d*),",x[0])[0]),                             # 4: planning_time(ms)
                                   int(re.findall(r"waiting: (\d*)\)",x[0])[0]),                             # 5: waiting_time(ms)
                                   int(re.findall(r"\s([-|\d]*)\sB",x[0])[0]),                               # 6: memory_usage(Byte)
                                   int(re.findall(r"\s(\d*)\spage hits,",x[0])[0]),                          # 7: page_hits
                                   int(re.findall(r",\s(\d*)\spage faults",x[0])[0]),                        # 8: page_faults
                                   re.findall(r"faults - (.*)",x[0])[0],                                     # 9: session
                                   re.sub(r'UNWIND .* as rows','UNWIND [parameters] as rows',re.split("{ -",x[1][::-1],maxsplit=2)[2][::-1].replace('\n',' ').strip()).replace('`',''),  # 10: query
                                   re.findall(r"- (\{.*?\}) -",x[1],flags=re.DOTALL)[0],                      # 11: parameters
                                   re.findall(r'} - (.*) - {',x[1])[0] ,                                     # 12: action
                                   re.split(r'\{ -',x[1][::-1],maxsplit=1)[0][::-1] if (re.findall(r"\d{2}:\d{2}:\d{2}\.\d{3}\+\d{4}\s(\w+)\s+",x[0])[0]!='ERROR') else re.split(r'-',x[1][::-1],maxsplit=1)[0][::-1], # 13: type
                                   re.findall(r"(.*?)\t",x[0][::-1])[0][::-1].strip(),           # 14: db
                                   re.findall(r"\d{2}:\d{2}:\d{2}\.\d{3}\+\d{4}\s(\w+)\s+",x[0])[0]          # 15: info  
                                  ])
            except:
                raise Exception('parsing 중 error 발생 transaction id:', re.findall(r"transaction id:(.*?)\s-",x[0])[0])
        return split_list
    
    def to_csv(self, data:list, file_down_path:str, file:str):
        col = ['date','query_id','transaction_id','elapsed_time(ms)','planning_time(ms)','waiting_time(ms)','memory_usage(Byte)','page_hits','page_faults','session','query','parameters','action','etc','db','type']
        export_df = pd.DataFrame(sorted(data, key=lambda x:(x[0],x[1],x[2])),columns=col)
#         export_df.to_csv('\\'.join(file_down_path.split('\\')[:-1])+'\\'+file_down_path.split('\\')[-1].replace('.','_')+'.csv',encoding="utf-8",index=False)
        export_df.to_csv(file_down_path+'\\'+file.split('\\')[-1].replace('.','_')+'.csv',encoding="utf-8",index=False)


    def add_filter(self, colname:str, operator: str, value: str):
        if colname in ['date','query_id','transaction_id','elapsed_time(ms)','planning_time(ms)','waiting_time(ms)','memory_usage(Byte)','page_hits','page_faults','session','query','parameters','action','etc','db','type']:
            if operator in ['!=','==','<=','>=','<','>']:
                if type(value)==str:
                    self.condition[colname].append([operator,value])
                else:
                    print(value, 'Please enter str type instead of ',type(value),'.')
            else:
                print('Not Expropriate Operator', operator)
        else:
            print('Not Exists Colnum Name, ',colname)
            
    def show_filter(self):
        for i in self.condition.items():
            print(i)

    def filtering(self, data:list, confition: dict):
        for i in confition.keys():
            for j in confition[i]:
                temp = eval("[ x for x in data if str("+self.col_dict[i]+")"+ j[0]+"'"+j[1]+"'"+"]")
                data = temp
        return data
    
if __name__ == "__main__":
    args = sys.argv
    if not(os.path.exists(args[1])):
        print('file_path dose not exists')
    if (args[2] == 'None'):
        args[2] = None
    elif not(os.path.exists(args[2])):
        print('download_path dose not exists')
    if (args[3].lower() == 'false'):
        args[3] = False
    elif (args[3].lower() == 'true'):
        args[3] = True
    else:
        print(' Not True or False')
    file_list = os.listdir(args[1])
    file_list = [ file for file in file_list if os.path.isfile(args[1]+'\\'+file)]
    file_list = [ args[1]+'\\'+file for file in file_list if '.log' in file]
    print(file_list)
    stats = CyStats(file_list = file_list, download=args[2], transaction_to_csv=args[3] )
    stats.excute()