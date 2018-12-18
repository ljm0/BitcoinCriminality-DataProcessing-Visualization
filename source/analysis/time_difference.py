import networkx as nx
import pandas as pd
import os
import time

start=time.time()

def time_difference(file_path,addr,filename,tag):

    G = nx.DiGraph(directed=True)
    G = nx.read_gpickle(file_path)

    Edges = G.edges()

    tran_from = []
    tran_to = []
    times = []
    values = []

    for eachedges in Edges:
        tran_from.append(eachedges[0])
        tran_to.append(eachedges[1])
        metrics = G.get_edge_data(eachedges[0],eachedges[1])
        value1 = metrics['value']
        time1 = metrics['time']
        values.append(value1)
        times.append(time1)

    data_edges = {
        'Edges':Edges,
        'From':tran_from,
        'To':tran_to,
        'Value':values,
        'Time':times
    }

    edges_info=pd.DataFrame(data_edges)
    edges_info1=edges_info.sort_values(by = 'Time',axis=0,ascending = True)

    Total_time_start=edges_info1.iloc[0].Time
    Total_time_end=edges_info1.iloc[-1].Time
    Total_time_diff = Total_time_end-Total_time_start

    from_loc = edges_info1.loc[edges_info1['From']==addr]
    to_loc = edges_info1.loc[edges_info1['To']==addr]

    sometime = from_loc.append(to_loc)
    sometime_new=sometime.sort_values(by = 'Time',axis = 0,ascending = True) 
    t_start=sometime_new.iloc[0].Time 
    t_end=sometime_new.iloc[-1].Time
    t_diff = t_end-t_start

    ratio = t_diff/Total_time_diff

    dic_time = {'File':filename,'Address':addr,'Total_start':Total_time_start,'Total_end':Total_time_end,'Total_difference':Total_time_diff,'Central_start':t_start,'Central_end':t_end,'Central_difference':t_diff,'Ratio':ratio,'Tag':tag}

    return dic_time

# file_path = "criminality//Porn_Input1.gpickle"
# tag = 1
# addr = '15LKRRYM2k2CCSGT76rNbQmciLZJSxKXAx'
# test = time_difference(file_path,addr,tag)
# print(test)


root = r'C:\Users\54589\Desktop\pylsde\shuju'
output_list=[]
positive_set = os.listdir(root+'\\criminality')
negative_set = os.listdir(root+'\\donor')

file1 = open('address_format','r')
dic_file = {}
for line in file1:
    dic_file[line.split(',')[1]+'_'+line.split(',')[2]+'.gpickle']=line.split(',')[0]
#print(dic_file)

for i in positive_set:
    tag = 1
    filename=i
    addr = dic_file.get(i)
    test = time_difference(root+'\\criminality\\'+i,addr,filename,tag)
    print(test)
    output_list.append(test)
with open("time_criminality.txt","a+") as f:
    f.write(str(output_list))
output_list = []

file2 = open('valid_negative','r')
dic_file2 = {}
for line in file2:
    dic_file2[line.split(',')[0]]=(line.split(',')[1][:-1])
#print(dic_file2)
for j in negative_set:
    tag = 0
    filename = j
    addr_t = j.split('_')[1][:-8]
    addr = dic_file2.get(addr_t)
    test = time_difference(root+'\\donor\\'+j,addr,filename,tag)
    print(test)
    output_list.append(test)
with open("time_donor.txt","a+") as f:
    f.write(str(output_list))
output_list = []

end=time.time()
print('running time =',end-start)
