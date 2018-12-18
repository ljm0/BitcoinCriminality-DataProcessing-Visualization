import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import time
import func_lsde as fl
import scipy.stats as stats

start=time.time()
file_path = "test.gpickle"
address='15LKRRYM2k2CCSGT76rNbQmciLZJSxKXAx'

def analysis_lsde(file_path,address,flag):

    #read gpickle

    G = nx.DiGraph(directed=True)
    G = nx.read_gpickle(file_path)
    important_nodes=[address]# import important nodes
    
    ##########################for nodes(addresses)

    take_value_nodes=fl.fun_nodes(G)
    #nodes_info,clu_coe_filter,avg_coe,cen_in_filter,cen_out_filter,degree_sort
    nodes_info=take_value_nodes[0]
    #print(take_value_nodes[1])

    ########################for edges(transaction records)

    take_value_edges=fl.fun_edges(G,important_nodes)
    #edgesinfo,(important nodes info, important nodes first time&last time&time difference),t-value(total)dataframe,polynomial parameters 
    edges_info=take_value_edges[0]
    #print(take_value_edges[1][1])
    #print(take_value_edges[1][0])
    #print(edges_info)

    ########################for graph characteristics

    graph_charac=fl.graph_characteristic(G,nodes_info,edges_info)
    #addr_total_number,trans_total_number,value_max,value_min,value_average,value_median,value_onefourth,value_threefourth


    ########################for specific nodes

    specific_node_charac = fl.analy_specific_node(important_nodes[0],nodes_info,edges_info)
    #somenode,somenode_as_from,somenode_as_to,connected addr info(as from),connected addr info2(as to)

    ########################for subgraph

    #G2=fl.subgraph_total(G)#subgraph

    ########################summary&analyze
    feature_dic = {}

    addr_total_number=graph_charac[0]
    trans_total_number=graph_charac[1]

    #show in dic
    feature_dic['Total addresses'] = addr_total_number
    feature_dic['Total Transaction'] = trans_total_number
    feature_dic['Normality check(p-value)'] = stats.shapiro(edges_info.Value)[1]#if p > 0.001, transaction value meet the normality requirements
    feature_dic['Significance test(p-value)'] = stats.pearsonr(edges_info.Time,edges_info.Value)[1] #if p>0.01, time and value has the high significance relationship, i.e. time and value have high relativity
    print('24/35 successful!')
    feature_dic['Percentage of cluster coefficient>0 addresses'] = 100*(take_value_nodes[1].shape[0])/addr_total_number
    feature_dic['Average cluster coefficient of the whole graph'] = take_value_nodes[2]
    feature_dic['Percentage of in centrality>0.01 addresses']= 100*(take_value_nodes[3].shape[0])/addr_total_number
    feature_dic['Percentage of out centrality>0.01 addresses']= 100*(take_value_nodes[4].shape[0])/addr_total_number   
    feature_dic['Max value']=graph_charac[2]
    feature_dic['Min value']=graph_charac[3]
    feature_dic['Average value']=graph_charac[4]
    feature_dic['Median value']=graph_charac[5]
    feature_dic['One fourth value']=graph_charac[6]
    feature_dic['Three fourth value']=graph_charac[7]
    feature_dic['The max degree of all addresses'] = len(take_value_nodes[5])-1
#    feature_dic['Polynomial parameters']=take_value_edges[3]
    ttt1 = len(take_value_edges[1][0][0])
    feature_dic['Important nodes information'] = ttt1
    feature_dic['Important nodes start-time']=take_value_edges[1][1][0][0]
    feature_dic['Important nodes end-time']=take_value_edges[1][1][0][1]
    feature_dic['Important nodes time difference']=take_value_edges[1][1][0][2]
    ttt2=len(specific_node_charac[1])
    ttt3=len(specific_node_charac[2])
    if ttt1 ==0:
        feature_dic['This node as from']=0
        feature_dic['This node as to']=0
    else:
        feature_dic['This node as from']=ttt2/ttt1
        feature_dic['This node as to']=ttt3/ttt1
    if specific_node_charac[4]==0:
        feature_dic['This node connected with(as from)']=0
    else:
        feature_dic['This node connected with(as from)']=ttt2/specific_node_charac[4]

    if specific_node_charac[3]==0:
        feature_dic['This node connected with(as to)']=0
    else:
        feature_dic['This node connected with(as to)']=ttt3/specific_node_charac[3]
    
    #print(take_value_edges[1][1][0])
    #print(len(take_value_edges[1][0][0]))
    #print(specific_node_charac[3])

    
    if flag == True:
        add_feature1=nx.pagerank(G)
        iadd1=0
        for v in add_feature1.values():
            if v>0.01:
                iadd1=iadd1+1
        #    print(iadd1)

        add_feature2=nx.transitivity(G)#Compute graph transitivity, the fraction of all possible triangles present in G.
         #Possible triangles are identified by the number of “triads” (two edges with a shared vertex).
        #print(add_feature2)
        print('26/35 successful!')

        add_feature3=nx.degree_assortativity_coefficient(G)#Assortativity measures the similarity of connections in the graph with respect to the node degree.
        add_feature4=nx.is_chordal(G.to_undirected())#A graph is chordal if every cycle of length at least 4 has a chord (an edge joining two nodes not adjacent in the cycle).
        print('28/35 successful!')
        #print(add_feature4)
        add_feature5=nx.is_weakly_connected(G)
        add_feature6=nx.is_strongly_connected(G)
        print('30/35 successful!')
        add_feature7=nx.global_efficiency(G.to_undirected())#The efficiency of a pair of nodes in a graph is the multiplicative inverse of the shortest path distance between the nodes.
        # The average global efficiency of a graph is the average efficiency of all pairs of nodes
        add_feature8=nx.is_branching(G)
        print('32/35 successful!')
        add_feature9=sorted(nx.immediate_dominators(G, address).items())
        add_domin=len(add_feature9)
        add_feature10=nx.is_simple_path(G, address)#A simple path in a graph is a nonempty sequence of nodes in which no node appears more than once in the sequence,
        # and each adjacent pair of nodes in the sequence is adjacent in the graph.
        print('34/35 successful!')
        add_feature11=nx.overall_reciprocity(G)# reciprocity for the whole graph
        print('35/35 successful!')
        #print(add_feature11)
        feature_dic['Percentage of page Rank >0.01 address'] = iadd1/addr_total_number
        feature_dic['Transitivity']=add_feature2
        feature_dic['Assortativity']=add_feature3
        feature_dic['Chordal']=add_feature4
        feature_dic['Strongly connected']=add_feature5
        feature_dic['Weakly connected']=add_feature6
        feature_dic['Global efficiency']=add_feature7
        feature_dic['Is branching'] = add_feature8
        feature_dic['Immediate dominator(numbers)']=add_domin
        feature_dic['Is simple path']=add_feature10
        feature_dic['Reciprocity']=add_feature11
        Header=['Total addresses','Total Transaction','Normality check(p-value)','Significance test(p-value)','Percentage of cluster coefficient>0 addresses',
        'Average cluster coefficient of the whole graph','Percentage of in centrality>0.01 addresses','Percentage of out centrality>0.01 addresses',
        'Max value','Min value','Average value','Median value','One fourth value','Three fourth value','The max degree of all addresses',
        'Important nodes information','Important nodes start-time','Important nodes end-time','Important nodes time difference','This node as from','This node as to',
        'This node connected with(as from)','This node connected with(as to)','Percentage of page Rank >0.01 address','Transitivity','Assortativity','Chordal','Strongly connected',
        'Weakly connected','Global efficiency','Is branching','Immediate dominator(numbers)','Is simple path','Reciprocity']
    else:
        print('Finished')
        Header=['Total addresses','Total Transaction','Normality check(p-value)','Significance test(p-value)','Percentage of cluster coefficient>0 addresses',
        'Average cluster coefficient of the whole graph','Percentage of in centrality>0.01 addresses','Percentage of out centrality>0.01 addresses',
        'Max value','Min value','Average value','Median value','One fourth value','Three fourth value','The max degree of all addresses',
        'Important nodes information','Important nodes start-time','Important nodes end-time','Important nodes time difference','This node as from','This node as to',
        'This node connected with(as from)','This node connected with(as to)']


    #print(somenodesinfo_dic)
    #res=stats.pearsonr(edges_info.Time,edges_info.Value)
    #print(res)
   



    end=time.time()
    print('running time =',end-start)
    
    return feature_dic,Header
    
test = analysis_lsde(file_path,address,True)
print(test[0])
