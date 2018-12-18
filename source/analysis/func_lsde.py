import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import csv
import time

#################################################for nodes

def degree_calculate(G):
    #calculate degree, in degree, out degree of each nodes, save as dataframe
    Nodes = G.nodes()

    result_de = []
    result_inde = []
    result_outde = []

    for eachnode in Nodes:
        #G.add_node(eachnode, degree = G.degree(eachnode))
        result_de.append(G.degree(eachnode))
        #G.add_node(eachnode, in_degree = G.in_degree(eachnode))
        result_inde.append(G.in_degree(eachnode))
        #G.add_node(eachnode, out_degree = G.out_degree(eachnode))
        result_outde.append(G.out_degree(eachnode))

    data_nodes = {
        'Nodes_data':Nodes,
        'Degree':result_de,
        'In_degree':result_inde,
        'Out_degree':result_outde,
    }

    datainformation = pd.DataFrame(data_nodes)
    return datainformation


def clustering_coefficient(G,flag=0):
    #calculate clustering coefficient of each nodes and the average value of the whole graph, find out the points whose clustering_coefficient>0
    G1 = G.to_undirected() #聚集系数：此参数反映聚集程度，十分有用。不适用于有向图, 转为无向图判定clustering coefficient is a measure of the degree to which nodes in a graph tend to cluster together
    clustering_coefficient = nx.average_clustering(G1)#clustering_coefficient 网络的平均聚集系数
    clustering_coefficient1 = nx.clustering(G1)# 每一点的聚集系数
    #The global clustering coefficient is the number of closed triplets (or 3 x triangles) over the total number of triplets (both open and closed).
    #按照图形理论，聚集系数是表示一个图形中节点聚集程度的系数，证据显示，在现实中的网络中，尤其是在特定的网络中，
    #由于相对高密度连接点的关系，节点总是趋向于建立一组严密的组织关系。在现实世界的网络，这种可能性往往比两个节点之间随机设立了一个连接的平均概率更大。
    #https://en.wikipedia.org/wiki/Clustering_coefficient
    #print(clustering_coefficient)
    #print(clustering_coefficient1)
    clu_k=[]
    clu_v=[]
    clu_k_filter=[]
    clu_v_filter=[]
    for k,v in clustering_coefficient1.items():
        clu_k.append(k)
        clu_v.append(v)
        if v>flag:
            clu_k_filter.append(k)
            clu_v_filter.append(v)

    data_clu = {
        'Nodes_data':clu_k,
        'Clustering_coefficient':clu_v
    }

    data_clu_filter = {
        'Nodes_data':clu_k_filter,
        'Clustering_coefficient':clu_v_filter
    }
    df_clu = pd.DataFrame(data_clu)
    df_clu_filter = pd.DataFrame(data_clu_filter)
    return df_clu,df_clu_filter,clustering_coefficient


def centralityfun(G,flag=0.01):
    #calculate centrality, in centrality and out centrality of each nodes, find out the points whose centrality>0.01
    cen = nx.degree_centrality(G) # centrality for nodes，图的中心性，反映某点的重要程度
    cen_in = nx.in_degree_centrality(G) #in_degree_centrality,入度中心性
    cen_out = nx.out_degree_centrality(G) #out_degree_centrality,出度中心性
    cen_k=[]
    cen_v=[]
    cen_in_k=[]
    cen_in_v=[]
    cen_out_k=[]
    cen_out_v=[]
    cen_in_k_filter=[]
    cen_in_v_filter=[]
    cen_out_k_filter=[]
    cen_out_v_filter=[]

    for k,v in cen.items():
        cen_k.append(k)
        cen_v.append(v)
    for k,v in cen_in.items():
        cen_in_k.append(k)
        cen_in_v.append(v)
        if v>flag:
            cen_in_k_filter.append(k)
            cen_in_v_filter.append(v)
    for k,v in cen_out.items():
        cen_out_k.append(k)
        cen_out_v.append(v)
        if v>flag:
            cen_out_k_filter.append(k)
            cen_out_v_filter.append(v)
    data_cen = {'Nodes_data':cen_k,'Centrality':cen_v}
    data_cen_in = {'Nodes_data':cen_in_k, 'Centrality_in':cen_in_v}
    data_cen_out = {'Nodes_data':cen_out_k, 'Centrality_out':cen_out_v}
    data_cen_in_filter = {'Nodes_data':cen_in_k_filter, 'Centrality_in_filter':cen_in_v_filter}
    data_cen_out_filter = {'Nodes_data':cen_out_k_filter, 'Centrality_out_filter':cen_out_v_filter}
    
    df_cen_temp=pd.merge(pd.DataFrame(data_cen),pd.DataFrame(data_cen_in),how='left',on='Nodes_data')
    df_cen=pd.merge(df_cen_temp,pd.DataFrame(data_cen_out),how='left',on='Nodes_data')
    df_cen_in_filter=pd.DataFrame(data_cen_in_filter)
    df_cen_out_filter=pd.DataFrame(data_cen_out_filter)

    return df_cen,df_cen_in_filter,df_cen_out_filter


def degree_histrogram(G):
    #calculate the times of the degree in a graph, from 0 to the highest degree
    degree_sort = nx.degree_histogram(G)
    return degree_sort

def degree_histrogram_draw(degree_sort):
    plt.figure(2)
    plt.bar(range(len(degree_sort)),degree_sort,color='rgb',tick_label=range(1,len(degree_sort)+1,1))
    plt.title(u'Histogram of Degree')
    plt.xlabel("degree(n)")
    plt.ylabel("times(n)")
    plt.savefig("data_bitcoin/pic2_histogram.tif")


def subgraph(G,k=3):
    #find out the subgraph which subpoints no less than k
    G2 = nx.k_core(G,k)
    return G2

def subgraph_draw_save(G2):
    #draw the subgraph
    nx.write_gpickle(G2,"data_bitcoin/gpickle_subgraph.gpickle")
    plt.figure(1)
    nx.draw(G2)
    plt.savefig("data_bitcoin/pic1_subgraph.tif")


#################################################for nodes

def edge_analysis(G):#transfer gpickle to dataframe

    Edges = G.edges()
    tran_from = []
    tran_to = []
    timescatter = []
    valuescatter = []
#    value_total = 0

    for eachedges in Edges:
        tran_from.append(eachedges[0])
        tran_to.append(eachedges[1])
        metrics = G.get_edge_data(eachedges[0],eachedges[1])
        value1 = metrics['value']
        time1 = metrics['time']
#        value_total = value1+value_total
        valuescatter.append(value1)
        timescatter.append(time1)
    
    data_edges = {
        'Edges':Edges,
        'From':tran_from,
        'To':tran_to,
        'Time':timescatter,
        'Value':valuescatter
    }

    edges_info=pd.DataFrame(data_edges)
    #edges_info.to_csv('data_bitcoin/edges_basic.csv', header=True, index=True)
    return edges_info

def find_time(Edgesdata,addr):#find the transaction records of a specific address, and sort by time, calculate the time difference between start and end time
    from_loc = Edgesdata.loc[Edgesdata['From']==addr]
    to_loc = Edgesdata.loc[Edgesdata['To']==addr]
    sometime = from_loc.append(to_loc)
#    return sometime
#    sometime=find_time(Edgesdata,'15LKRRYM2k2CCSGT76rNbQmciLZJSxKXAx','1N76PRJADGA69ckwRHi32xk34G8MpAb5Gc')
#    print(sometime)
#    def sort_time(sometime):
    sometime_new=sometime.sort_values(by = 'Time',axis = 0,ascending = True) 
    try:
        t_start=sometime_new.iloc[0].Time 
        t_end=sometime_new.iloc[-1].Time
    except IndexError:
        t_start=0
        t_end=0
    return sometime_new,[t_start,t_end,t_end-t_start]

def time_sort(edges_info):#sort time, ascending order
    edges_info_filtered = edges_info.sort_values(by = 'Time',axis = 0,ascending = True)
    return edges_info_filtered

def scatter_draw(edges_info_filtered):#draw time-value scatter
    t_sca=edges_info_filtered.Time
    v_sca=edges_info_filtered.Value
    plt.figure(3)
    plt.plot(t_sca,v_sca,'ro')
    plt.title(u'time-value scatter')
    plt.ylabel("value(satoshi)")
    plt.xlabel("time(ms)")
    plt.savefig("data_bitcoin/pic3_time_value.tif")

def scatter_total(edges_info_filtered):#sum the value of the same time
    data_tv={
        'Time':edges_info_filtered.Time,
        'Value':edges_info_filtered.Value
    }
    tv=pd.DataFrame(data_tv)
    tvtotal=tv.groupby('Time').sum().reset_index()
    return tvtotal

def scatter_total_draw(tvtotal):#draw the time-value graph
    # l1=np.array(tvtotal.Time)
    # l1_list=l1.tolist()
    # l2=np.array(tvtotal.Value)
    # l2_list=l2.tolist()
    # print(l1_list)
    # print(l2_list)
    plt.figure(4)
    plt.plot(tvtotal.Time,tvtotal.Value,'ro')
    plt.bar(tvtotal.Time,tvtotal.Value,color='rgb')
    plt.title(u'time-value total')
    plt.ylabel("value(satoshi)")
    plt.xlabel("time(ms)")
    plt.savefig("data_bitcoin/pic4_time_value_total.tif")   


def poly_fitting(tvtotal,fit=3):#polynomial fitting
    z1 = np.polyfit(tvtotal.Time,tvtotal.Value,fit)
    # plt.figure(5)
    # plt.title(u'time-value polynomial curve')
    # plt.ylabel("value(satoshi)")
    # plt.xlabel("time(ms)")
    # plt.plot(tvtotal.Time,np.polyval(z1,tvtotal.Time))
    # plt.savefig("data_bitcoin/pic5_polynomial.tif")
    return z1


def graph_characteristic(G,nodes_info,edges_info):
    addr_total_number = G.number_of_nodes() #total address
    trans_total_number = G.number_of_edges() #total transaction amount
    print('12/35 successful!')
    L=edges_info.Value
    L=sorted(L)
    value_max=max(L)
    value_min=min(L)
    print('14/35 successful!')
    value_average=np.mean(L)
    value_median=np.median(L)
    value_onefourth=np.percentile(L,25)
    value_threefourth=np.percentile(L,75)
    # print(addr_total_number)
    # print(trans_total_number)
    # print(value_max)
    # print(value_min)
    # print(value_average)
    # print(value_median)
    # print(value_onefourth)
    # print(value_threefourth)
    print('20/35 successful!')
    return addr_total_number,trans_total_number,value_max,value_min,value_average,value_median,value_onefourth,value_threefourth
#    print(value_median)
#    return addr_total_number,trans_total_number,value_max,value_min

def specific_address_info(addr,nodes_info):
    somenode=nodes_info.loc[nodes_info['Nodes_data']==addr]
#    print(somenode)
    return somenode

def specific_address_connection_asfrom(addr,edges_info):
    from_loc=[]
    from_loc = edges_info.loc[edges_info['From']==addr]
    connect_edges_asfrom=np.array(from_loc.To)
    connect_edges_asfrom=connect_edges_asfrom.tolist()
    return from_loc,connect_edges_asfrom

def specific_address_connection_asto(addr,edges_info):
    to_loc=[]
    to_loc = edges_info.loc[edges_info['To']==addr]
    connect_edges_asto=np.array(to_loc.From)
    connect_edges_asto=connect_edges_asto.tolist()
    return to_loc,connect_edges_asto

def fun_nodes(G):
    degree_info=degree_calculate(G)#degree calculator
    #print(degree_info)
    #degree_info.to_csv('data_bitcoin/data0_characteristics.csv', header=True, index=True)
    
    (clu_coe,clu_coe_filter,avg_coe)=clustering_coefficient(G,0)#clustering coefficient calculator,find out >0 nodes
    #clu_coe_filter.to_csv('data_bitcoin/data1_clustering_coefficient.csv', header=True, index=True)
    #print(clu_coe)
    # #print(clu_coe_filter)
    # #print(avg_coe)
    print('2/35 successful!')

    (centrality,cen_in_filter,cen_out_filter) =centralityfun(G,0.01)#centrality calculator, find out >0.01 nodes
    print('4/35 successful!')
    #cen_in_filter.to_csv('data_bitcoin/data2_cen_in_filter.csv', header=True, index=True)
    #cen_out_filter.to_csv('data_bitcoin/data3_cen_out_filter.csv', header=True, index=True)
    #print(centrality)
    #print(cen_in_filter)
    #print(cen_out_filter)

    df1=pd.merge(degree_info,clu_coe,how='left',on='Nodes_data')#output all the calculated characteristics, combine them into one dataframe
    nodes_info=pd.merge(df1,centrality,how='left',on='Nodes_data')
    #nodes_info.to_csv('data_bitcoin/data0_characteristics.csv', header=True, index=True)
    #print(nodes_info)

    degree_sort=degree_histrogram(G)
    #degree_histrogram_draw(degree_sort)
    #print(degree_sort)

    return nodes_info,clu_coe_filter,avg_coe,cen_in_filter,cen_out_filter,degree_sort

def subgraph_total(G):    
    G2 = subgraph(G,3)
    subgraph_draw_save(G2)
    return G2

def analy_importantnodes_infoandtime(edges_info,important_nodes):
    inodes = []
    tnodes = [[0,0,0]]
    for eachaddress in important_nodes:
        some_edge_time=find_time(edges_info,eachaddress)#find the transaction records of a specific address, and sort by time, calculate the time difference between start and end time
        inodes.append(some_edge_time[0])
        tnodes[0]=some_edge_time[1]
        # print(some_edge_time[0])
        # print('t start=',some_edge_time[1][0])
        # print('t end=',some_edge_time[1][1])
        # print('time difference=',some_edge_time[1][2])
    #print(inodes)
    #print(tnodes)
    #print(tnodes)
    return inodes,tnodes

def fun_edges(G,important_nodes):
    edges_info=edge_analysis(G)
    (inodes,tnodes)=analy_importantnodes_infoandtime(edges_info,important_nodes)
    print('8/35 successful!')

    #print(edges_info)
    #some_edge_time=fl.find_time(edges_info,important_nodes)#find the transaction records of a specific address, and sort by time, calculate the time difference between start and end time
    # print(some_edge_time[0])
    # print(some_edge_time[1][0])
    edges_info_filtered = time_sort(edges_info) #sort time, ascending order
    #edges_info_filtered.to_csv('data_bitcoin/edges_filtered.csv', header=True, index=True)

    #scatter_draw(edges_info_filtered)#draw time-value scatter
    tvtotal=scatter_total(edges_info_filtered)#find out total value of the same time
    #scatter_total_draw(tvtotal)
    #print(tvtotal)

    #para=poly_fitting(tvtotal,3)#save the parameter and draw the pic
    #print(inodes)
    print('10/35 successful!')
    return edges_info_filtered,(inodes,tnodes),tvtotal#,para


def for_specific_node(specific_node,edges_info):
    (somenode_as_from,connect1)=specific_address_connection_asfrom(specific_node,edges_info)
    (somenode_as_to,connect2)=specific_address_connection_asto(specific_node,edges_info)
    print('22/35 successful!')
    return somenode_as_from,connect1,somenode_as_to,connect2

def analy_specific_node(specific_node,nodes_info,edges_info):
    somenode=specific_address_info(specific_node,nodes_info)
    #print(somenode)

    (somenode_as_from,connect1,somenode_as_to,connect2)=for_specific_node(specific_node,edges_info)
    #print(temp[0])
    #print(connect2)
    #print(somenode_as_to)

    Laddr1=0
    for eachaddr in connect1:
        temp1=int(specific_address_info(eachaddr,nodes_info).Degree)
        Laddr1=Laddr1+temp1
    #    print(temp1)

    Laddr2=0
    for eachaddr in connect2:
        temp2=int(specific_address_info(eachaddr,nodes_info).Degree)
        Laddr2=Laddr2+temp2
    #    print(temp2)
    
    return somenode,somenode_as_from,somenode_as_to,Laddr1,Laddr2