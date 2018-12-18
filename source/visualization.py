import networkx as nx
def draw_network(central_address,max_degree,choose_degree,file_dir):
    g=nx.DiGraph()
    data=read_file(choose_degree,file_dir)

def read_file(current_degree,file_dir):
    f=open(file_dir+"/"+str(current_degree),"r+")
    data=f.readlines()
    return data