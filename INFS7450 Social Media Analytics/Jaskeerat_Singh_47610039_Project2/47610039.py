#Importing Libraries
import random
import pandas as pd
import numpy as np
import networkx as nx
import warnings
warnings.filterwarnings("ignore")

from node2vec import Node2Vec
from sklearn.ensemble import RandomForestRegressor


#Reading Data as graph and dataframe.
def read_as_graph(data_path):
    df = pd.read_csv(data_path, sep = " ", header = None, names =  ["node1", "node2"], dtype = {"node1": "Int32", "node1": "Int32"})
    
    graph = nx.Graph()
    for i in list(df.index):
        graph.add_edge(df["node1"][i], df["node2"][i])
    
    print("For File:", data_path, "|| Number of Data Rows:", df.shape[0], "|| Number of Edges in Graph:", len(graph.edges()))
    return graph, df


#Checking for Accuracy against validation positive set and returning percentage.
def validation(ground_truth_edges, predicted):
    count = 0
    for i in predicted:
        if(i in ground_truth_edges):
            count = count + 1
    
    accuracy = (count / len(ground_truth_edges)) * 100
    return accuracy


#Applying Neighborhood Based Methods and storing it as dataframe. Finally returning the accuracy percentage and top 100 predicted edges.
def top100neighbor(method, graph, edges_df, ground_truth_edges):
    score_df = pd.DataFrame(columns = ["edge", "score"])
    
    for i in edges_df.index:
        if(method == "jaccard"):
            score = list(nx.jaccard_coefficient(graph, ebunch = [ (edges_df["node1"][i], edges_df["node2"][i]) ]))
        elif(method == "adamic"):
            score = list(nx.adamic_adar_index(graph, ebunch = [ (edges_df["node1"][i], edges_df["node2"][i]) ]))
        elif(method == "preferential"):
            score = list(nx.preferential_attachment(graph, ebunch = [ (edges_df["node1"][i], edges_df["node2"][i]) ]))
        elif(method == "allocation"):
            score = list(nx.resource_allocation_index(graph, ebunch = [ (edges_df["node1"][i], edges_df["node2"][i]) ]))
        
        score_df = score_df.append({"edge": tuple(sorted([score[0][0], score[0][1]])), "score": score[0][2]}, ignore_index = True)
    
    score_df.sort_values(by = ["score"], inplace = True, ascending = False)
    
    predicted = list(score_df["edge"].head(100))
    accuracy = validation(ground_truth_edges, predicted)
    
    return predicted, accuracy


#Applying Walk Based Methods and storing it as dataframe. Finally returning the accuracy percentage and top 100 predicted edges.
def top100walk(method, graph, edges_list, ground_truth_edges):
    
    if(method == "simrank"):
        score = nx.simrank_similarity(graph)
    elif(method == "shortestdistance"):
        score = dict(nx.shortest_path_length(graph))
    
    score_df = pd.DataFrame(score).stack().reset_index()
    score_df.columns = ["node1", "node2", "score"]
    
    score_df["edge"] = score_df.apply(lambda row: tuple(sorted( [int(row["node1"]), int(row["node2"])] )), axis = 1)
    score_df = score_df[score_df["edge"].isin(edges_list)]
    
    score_df = score_df[["edge", "score"]]
    score_df = score_df.groupby(["edge"], as_index = False).mean()
    
    if(method == "simrank"):
        score_df.sort_values(by = ["score"], ascending = False, inplace = True)
    elif(method == "shortestdistance"):
        score_df.sort_values(by = ["score"], inplace = True)
    
    predicted = list(score_df["edge"].head(100))
    accuracy = validation(ground_truth_edges, predicted)
    
    return predicted, accuracy


#Applying Embedding Based Method Node2vec and storing it as dataframe. Finally returning the accuracy percentage and top 100 predicted edges.
#workers is set to 4 for multi cpu core processing and reducing time it takes to process. Setting it to 1 might cause the program to run over the time limit.
def embedding(training_graph, validation_graph, testing_graph, edges_not_available):
    
    node2vec = Node2Vec(training_graph, dimensions = 128, walk_length = 80, num_walks = 150, workers = 4, p = 0.5, q = 0.25)
    model = node2vec.fit(window = 10, min_count = 1)
    
    training_matrix = []
    labels = []

    for i in training_graph.edges():
        features = np.concatenate([ model.wv[str(i[0])], model.wv[str(i[1])] ])
        training_matrix.append(features)
        labels.append(1)

    for i in edges_not_available:
        features = np.concatenate([ model.wv[str(i[0])], model.wv[str(i[1])] ])
        training_matrix.append(features)
        labels.append(0)
    
    validation_matrix = []
    score_df = pd.DataFrame(columns = ["edge"])
    for i in validation_graph.edges():
        edge = tuple(sorted([ i[0], i[1] ]))
        score_df = score_df.append({"edge": edge}, ignore_index = True)
        features = np.concatenate([ model.wv[str(edge[0])], model.wv[str(edge[1])] ])
        validation_matrix.append(features)
    
    rf_model = RandomForestRegressor(n_estimators = 200, random_state = 42, criterion = "poisson", max_features = "log2", n_jobs = 10)
    rf_model.fit(training_matrix, labels)
    y_pred_proba = rf_model.predict(validation_matrix)
    
    score_df["score"] = y_pred_proba
    score_df.sort_values(by = ["score"], ascending = False, inplace = True)
    
    predicted = list(score_df["edge"].head(100))
    accuracy = validation(ground_truth_edges, predicted)
    
    return predicted, accuracy


#Predicting Test Edges from the devised strategy
def final_predictions(training_graph, testing_df):
    adamic_df = pd.DataFrame(columns = ["edge", "score"])
    for i in testing_df.index:
        score = list(nx.adamic_adar_index(training_graph, ebunch = [ (testing_df["node1"][i], testing_df["node2"][i]) ]))
        adamic_df = adamic_df.append({"edge": tuple(sorted([score[0][0], score[0][1]])), "score": score[0][2]}, ignore_index = True)

    adamic_df.sort_values(by = ["score"], inplace = True, ascending = False)
    adamic_predicted = list(adamic_df["edge"].head(100))

    jaccard_df = pd.DataFrame(columns = ["edge", "score"])
    for i in testing_df.index:
        score = list(nx.jaccard_coefficient(training_graph, ebunch = [ (testing_df["node1"][i], testing_df["node2"][i]) ]))
        jaccard_df = jaccard_df.append({"edge": tuple(sorted([score[0][0], score[0][1]])), "score": score[0][2]}, ignore_index = True)

    jaccard_df.sort_values(by = ["score"], inplace = True, ascending = False)
    jaccard_predicted = list(jaccard_df["edge"].head(100))
    
    common_edges = set(adamic_predicted).intersection(jaccard_predicted)

    adamic_not_selected = set(adamic_predicted) - common_edges
    adamic_df = adamic_df[adamic_df["edge"].isin(adamic_not_selected)]
    adamic_df.sort_values(by = ["score"], ascending = False, inplace = True)
    adamic_not_selected = list(adamic_df["edge"].head(13))

    jaccard_not_selected = set(jaccard_predicted) - common_edges
    jaccard_df = jaccard_df[jaccard_df["edge"].isin(jaccard_not_selected)]
    jaccard_df.sort_values(by = ["score"], ascending = False, inplace = True)
    jaccard_not_selected = list(jaccard_df["edge"].head(6))

    final_edges = list(common_edges) + list(adamic_not_selected) + list(jaccard_not_selected)
    
    return final_edges


#Writing top 100 edges as a .txt file
def write_results(edges):
    
    with open("47610039.txt", "w") as results:
        for i in edges:
            if(edges.index(i) != len(edges)-1):
                results.write(str(i[0]) + " " + str(i[1]) + "\n")
            else:
                results.write(str(i[0]) + " " + str(i[1]))
        results.close()
    
    return True


#Writing results for kaggle in a .csv file
def kaggle_results(mappings, predicted_test_edges):
    
    kaggle_df = pd.read_csv(mappings)
    kaggle_df["edge"] = kaggle_df.apply(lambda x: tuple(sorted([int(i) for i in x["edge"].split(" ")])), axis = 1)
    kaggle_df["label_0"] = None
    
    for i in predicted_test_edges:
        kaggle_df.loc[kaggle_df["edge"] == i, "label_0"] = 1
    
    kaggle_df = kaggle_df[["edge_id", "label_0"]]
    kaggle_df["label_0"].fillna(0, inplace=True)
    kaggle_df.to_csv("kaggle.csv", index = False)
    
    return True


#Defining Main Task
if __name__ == "__main__":
    
    #File Locations based on the fact that all the files in a folder 'data' and this folder is in the same place as this .py file
    training_file = "./data/training.txt"
    testing_file = "./data/test.txt"
    validation_positive_file = "./data/val_positive.txt"
    validation_negative_file = "./data/val_negative.txt"

    #Reading as Graph and DataFrame
    training_graph, training_df = read_as_graph(training_file)
    testing_graph, testing_df = read_as_graph(testing_file)

    val_positive_graph, val_positive_df = read_as_graph(validation_positive_file)
    val_positive_df["label"] = 1
    val_negative_graph, val_negative_df = read_as_graph(validation_negative_file)
    val_negative_df["label"] = 0

    validation_graph = nx.compose(val_positive_graph, val_negative_graph)
    validation_df = pd.concat([val_positive_df, val_negative_df], ignore_index = True)
    
    #For Validation Checking
    ground_truth_edges = [tuple(sorted([ int(i[0]), int(i[1]) ]))  for i in val_positive_graph.edges()]
    validation_edges = [ tuple(sorted([ int(i[0]), int(i[1]) ])) for i in validation_graph.edges() ]
    
    #Neighborhood Based Methods
    jaccard_predicted, accuracy = top100neighbor("jaccard", training_graph, validation_df, ground_truth_edges)
    print("Jaccard Accuracy:", accuracy)
    
    adamic_predicted, accuracy = top100neighbor("adamic", training_graph, validation_df, ground_truth_edges)
    print("Adamic-Adar Accuracy:", accuracy)
    
    preferential_predicted, accuracy = top100neighbor("preferential", training_graph, validation_df, ground_truth_edges)
    print("Preferential Accuracy:", accuracy)
    
    allocation_predicted, accuracy = top100neighbor("allocation", training_graph, validation_df, ground_truth_edges)
    print("Allocation Accuracy:", accuracy)
    
    #Walk Based Methods
    simrank_predicted, accuracy = top100walk("simrank", training_graph, validation_edges, ground_truth_edges)
    print("Simrank Accuracy:", accuracy)
    
    shortest_predicted, accuracy = top100walk("shortestdistance", training_graph, validation_edges, ground_truth_edges)
    print("Shortest Distance Accuracy:", accuracy)
    
    #Embedding Based Method
    total_graph = nx.compose(nx.compose(training_graph, validation_graph), testing_graph)
    total_available_edges = set([tuple(sorted(list(i))) for i in total_graph.edges()])
    complete_edges = set([tuple(sorted(list(i))) for i in nx.complete_graph(total_graph).edges()])
    
    edges_not_available = list(complete_edges - total_available_edges)
    random.seed(42)
    edges_not_available = random.sample(edges_not_available, 20000)
    
    node2vec_predicted, accuracy = embedding(training_graph, validation_graph, testing_graph, edges_not_available)
    print("Node2Vec Accuracy:", accuracy)
    
    #########################################################################################################################
    ##### We can see that the accuracy of Adamic-Adar and Jaccard is the highest, hence using combination as prediction #####
    #########################################################################################################################
    
    predicted_test_edges = final_predictions(training_graph, testing_df)
    write_results(predicted_test_edges)
    
    #Writing File for Kaggle Submission
    #kaggle_results("edge_map.csv", predicted_test_edges)