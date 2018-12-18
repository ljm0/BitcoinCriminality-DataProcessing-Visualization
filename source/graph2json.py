import networkx
import json
#f=open("demo.json",'r')
# json.loads(f.readlines())
# for i in f.readlines():
#     print(i)
# print()
g=networkx.read_gpickle("structures\Porn_Input1.gpickle")
g1=g.to_undirect()
nodes_list=[]
for i in g.nodes:
    print(i,g.degree(i))
    distance=0
    try:
        distance=len(networkx.shortest_path(g1, source=i, target="15LKRRYM2k2CCSGT76rNbQmciLZJSxKXAx"))-1
        print(distance)
    except:
        distance=0
    tmp_dic={
            "name": i,
            "symbolSize": g.in_degree(i)**0.5,
            "draggable": "False",
            "value": g.in_degree(i,weight="value"),
            "category": distance,
            "label": {
                "normal": {
                    "show": "True"
                }
            }
        }
    nodes_list.append(tmp_dic)
#g=networkx.DiGraph()
print(nodes_list)
edge_list=[{"source":i[0],"target":i[1]} for i in g.edges]
with open("text.json",'a+') as f:
    json.dump([nodes_list,edge_list],f)
# for i in a:
#     print(i)