import networkx
import matplotlib.pyplot as plt
import pandas as pd
import time
def parser(type_name,tag,file_name):
    f=open(type_name+'/'+tag+'/'+str(file_name),"r")
    edge=[]
    for transaction_string in f.readlines():
        transaction=eval(transaction_string)
        inputs=transaction[0]
        outputs=transaction[1]
        trans_time=transaction[2]
        for input_node in inputs:
            if input_node==None:
                continue
            for output_node in outputs:
                # print((input_node,output_node,trans_time))
                if output_node==None:
                    continue
                yield ((input_node[0], output_node[0], {"value": output_node[1], "time": trans_time}))
# edges_df=pd.DataFrame(parser("Porn","Input0",2))
# edges_df.to_csv('Porn/Intput0/edges.csv',sep=' ',header=False)
def save_structure(type_name,tag):
    print(tag)
    #start = time.time()
    dirct_graph = networkx.DiGraph(directed=True)
    dirct_graph.add_edges_from(parser(type_name, tag, 1))
    dirct_graph.add_edges_from(parser(type_name, tag, 2))
    networkx.write_gpickle(dirct_graph, "structures/"+type_name+"_"+tag+".gpickle")
    #print(dirct_graph.edges)
    #end = time.time()
    #print(start - end)

# for edge in parser("Porn","Input0",1):
#     dirct_graph.add_edge(edge[0],edge[1])
# for edge in parser("Porn","Input0",2):
#     dirct_graph.add_edge(edge[0],edge[1])
# networkx.draw(dirct_graph)
#
# plt.show()

