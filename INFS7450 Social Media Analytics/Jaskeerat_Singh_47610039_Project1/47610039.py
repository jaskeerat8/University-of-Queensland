#Importing Libraries
import pandas as pd
import numpy as np
import networkx as nx
from collections import deque
#import matplotlib.pyplot as plt

import warnings
warnings.filterwarnings("ignore")



def read_data(data_path):
    
    #Reading the txt file as DataFrame
    df = pd.read_csv(data_path, sep = " ", header = None, names =  ["node1", "node2"], dtype = {"node1":"Int64", "node2":"Int64"})
    
    #Making a Graph and adding edges
    undirected_graph = nx.Graph()
    for i in df.index:
        undirected_graph.add_edge(df["node1"][i], df["node2"][i])
    
    #plt.figure(1, figsize = (100, 60), dpi = 10)
    #nx.draw(undirected_graph)
    #plt.show()
    
    print("Number of Nodes:", undirected_graph.number_of_nodes())
    print("Number of Edges:", undirected_graph.number_of_edges())
    
    return undirected_graph



def shortest_bfs(dict_graph, all_nodes, start_node):
    
    #Defining the dictionaries for number of paths, parents and visiting sequence
    source_dict = {}
    for i in all_nodes:
        source_dict[i] = []
    
    routecount = dict(zip(all_nodes, [0] * len(all_nodes)))
    routecount[start_node] = 1
    
    distance = dict(zip(all_nodes, [np.NINF] * len(all_nodes)))
    distance[start_node] = 0
    
    q = deque([start_node])
    visit_sequence = deque()

    while(len(q) != 0):

        latest_node = q.popleft()
        visit_sequence.append(latest_node)

        latest_node_distance = distance[latest_node]
        latest_node_neighbors = dict_graph[latest_node]

        for neighbor in latest_node_neighbors:
            if(distance[neighbor] == np.NINF):
                q.append(neighbor)
                distance[neighbor] = latest_node_distance + 1

            if(distance[neighbor] == latest_node_distance + 1):
                routecount[neighbor] = routecount[neighbor] + routecount[latest_node]
                source_dict[neighbor].append(latest_node)

    neighbours_contributions = {}
    for i in visit_sequence:
        neighbours_contributions[i] = 0
    
    return source_dict, routecount, visit_sequence, neighbours_contributions



def betweeness_centrality(undirected_graph):
    
    #Getting list of all the nodes in the Graph
    all_nodes = list(undirected_graph.nodes())
    
    #Convert networkx graph to dictionary
    dict_graph = {}
    for i in undirected_graph.nodes():
        dict_graph[i] = list(undirected_graph.neighbors(i))
    
    #defining dictionary to store centrality of each node
    centrality = {}
    for i in all_nodes:
        centrality[i] = 0
    
    #Getting Centrality of node one by one
    for start_node in all_nodes:
        
        #Getting the source of each node, number of paths and sequence of visiting all other nodes
        source_dict, path_sum, visit_sequence, neighbours_contributions = shortest_bfs(dict_graph, all_nodes, start_node)
        
        while(len(visit_sequence) != 0):
            
            #Applying the Betweeness Centrality formula
            node = visit_sequence.pop()
            node_coeff = (1 + neighbours_contributions[node]) / path_sum[node]
            
            for parent_node in source_dict[node]:
                neighbours_contributions[parent_node] = neighbours_contributions[parent_node] + (path_sum[parent_node] * node_coeff)
            
            if(node != start_node):
                centrality[node] = centrality[node] + neighbours_contributions[node]
    
    #Saving the nodes and their centrality in a DataFrame and getting top 10 nodes
    dataframe_dictionary = {"node": centrality.keys(), "centrality": centrality.values()}
    df = pd.DataFrame(dataframe_dictionary)
    df.sort_values("centrality", ascending = False, inplace = True)
    final_nodes = list(df.head(10)["node"])
    
    #Returning list of top 10 nodes
    return final_nodes



def pagerank_centrality(undirected_graph):
    
    #Reading the Graph as adjacency matrix and diagnol matrix
    adj_matrix = nx.adjacency_matrix(undirected_graph).toarray()
    diag_matrix = np.diag(np.sum(adj_matrix, axis=1))
    adj_transpose_matrix = adj_matrix.transpose()
    diag_inverse_matrix = np.linalg.inv(diag_matrix)
    
    #Setting the Value of parameters
    alpha = 0.85
    beta = 0.15
    condition = 1e-10

    n = len(undirected_graph.nodes())
    ones_array = np.ones(n)
    identity_matrix = np.identity(n)
    
    #Applying the Pagerank Centrality formula
    last_iteration = np.zeros(n)
    current_iteration = ones_array / n
    
    while(np.abs(last_iteration - current_iteration).sum() > condition):
        last_iteration = current_iteration
        current_iteration = beta * (np.linalg.inv(identity_matrix - (alpha * (adj_transpose_matrix @ diag_inverse_matrix)))) @ ones_array
        current_iteration = current_iteration / np.sum(current_iteration)
    
    #Saving the nodes and their centrality in a DataFrame and getting top 10 nodes
    dataframe_dictionary = {"node": list(undirected_graph.nodes()), "centrality": current_iteration}
    df = pd.DataFrame(dataframe_dictionary)
    df.sort_values("centrality", ascending = False, inplace = True)
    final_nodes = list(df.head(10)["node"])
    
    #Returning list of top 10 nodes
    return final_nodes



def write_results(bc_list, pr_list):
    
    #Saving Main Final Results to a .txt file
    with open("47610039.txt", "w") as results:
        results.write(" ".join([str(i) for i in bc_list]) + "\n")
        results.write(" ".join([str(i) for i in pr_list]))
        results.close()
    
    #Saving Kaggle Results by making a DataFrame
    df = pd.DataFrame(columns=["Id", "Label"])
    
    for i in range(0, 4039):
        if((i in bc_list) and (i in pr_list)):
            df = df.append({"Id":i, "Label":3}, ignore_index = True)
        elif(i in bc_list):
            df = df.append({"Id":i, "Label":1}, ignore_index = True)
        elif(i in pr_list):
            df = df.append({"Id":i, "Label":2}, ignore_index = True)
        elif((i not in bc_list) and (i not in pr_list)):
            df = df.append({"Id":i, "Label":0}, ignore_index = True)
        else:
            continue
    
    #df.to_csv("47610039.csv", index = False)
    
    return True



if __name__ == "__main__":
    
    #Name of the RAW Data File
    raw_data = "../3. data.txt"
    
    #Reading the data and making a graph
    undirected_graph = read_data(raw_data)
    
    #Calculating the Betweeness Centrality and returning the list of top 10 nodes
    bc_list = betweeness_centrality(undirected_graph)
    print("Betweeness Centrality List:", str(bc_list))
    
    #Calculating the Pagerank Centrality and returning the list of top 10 nodes
    pr_list = pagerank_centrality(undirected_graph)
    print("PageRank Centrality List:", str(pr_list))
    
    #Writing the results for both kaggle and final txt file
    write_results(bc_list, pr_list)