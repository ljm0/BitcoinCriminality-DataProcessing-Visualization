import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import func_lsde as fl

file_path="Porn/Sextortion_Sex15.gpickle"
def liulaoge(file_path):
    G = nx.DiGraph(directed=True)
    G = nx.read_gpickle(file_path)
    G2=fl.subgraph_total(G)#subgraph
