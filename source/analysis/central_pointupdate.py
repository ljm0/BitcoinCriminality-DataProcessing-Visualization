import networkx as nx
import pandas as pd
import os
import time

start=time.time()

def central_points(file_path,addr,filename,tag):

    G = nx.DiGraph(directed=True)
    G = nx.read_gpickle(file_path)

    Nodes = G.nodes()

    nodes_number = G.number_of_nodes() #total address

    result_outde = []
    for eachnode in Nodes:
        result_outde.append(G.out_degree(eachnode))

    data_nodes = {
        'Nodes_data':Nodes,
        'Out_degree':result_outde,
    }
    basic_nodes_info = pd.DataFrame(data_nodes)

    nodes_info = basic_nodes_info[basic_nodes_info['Out_degree']>=100]
    total100=len(nodes_info)
    dic_nodes = {'File':filename,'Address':addr,'Total_nodes':nodes_number,'Total>=100':total100,'0':0,'1':0,'2':0,'3':0,'4':0,'5':0,'6':0,'7':0,'other1':0,'other2':0,'Tag':tag}

    if len(nodes_info) == 0:
        return dic_nodes

    L =nodes_info.Nodes_data
    G1=G.to_undirected()

    for eachone in L:
        if eachone==addr:
            dic_nodes['0']+=1
        else:
            try:
                distance = nx.dijkstra_path_length(G, source=eachone, target=addr)
                if distance==1:
                    dic_nodes['5']+=1
                elif distance==2:
                    dic_nodes['6']+=1
                else:
                    distance = nx.dijkstra_path_length(G1, source=eachone, target=addr)#2or4join6
                    if distance==2:
                        dic_nodes['2']+=1
                    elif distance==3:
                        dic_nodes['4']+=1
                    else:
                        dic_nodes['other1']+=1
            except nx.NetworkXNoPath:
                distance = nx.dijkstra_path_length(G1, source=eachone, target=addr)
                if distance==1:
                    dic_nodes['1']+=1
                elif distance==2:
                    dic_nodes['2']+=1
                elif distance==3:
                    dic_nodes['4']+=1
                else:
                    dic_nodes['other2']+=1


    return dic_nodes

root = r'C:\Users\54589\Desktop\pylsde\shuju'
output_list=[]
positive_set = os.listdir(root+'\\criminality')
negative_set = os.listdir(root+'\\donor')

file1 = open('address_format','r')
dic_file = {}
for line in file1:
    dic_file[line.split(',')[1]+'_'+line.split(',')[2]+'.gpickle']=line.split(',')[0]

for i in positive_set:
    tag = 1
    filename=i
    addr = dic_file.get(i)
    test = central_points(root+'\\criminality\\'+i,addr,filename,tag)
    print(test)
    output_list.append(test)
with open("newpoints_criminality.txt","a+") as f:
    f.write(str(output_list))
output_list = []

file2 = open('valid_negative','r')
dic_file2 = {}
for line in file2:
    dic_file2[line.split(',')[0]]=(line.split(',')[1][:-1])
for j in negative_set:
    tag = 0
    filename = j
    addr_t = j.split('_')[1][:-8]
    addr = dic_file2.get(addr_t)
    test = central_points(root+'\\donor\\'+j,addr,filename,tag)
    print(test)
    output_list.append(test)
with open("newpoints_donor.txt","a+") as f:
    f.write(str(output_list))
output_list = []

end=time.time()
print('running time =',end-start)
