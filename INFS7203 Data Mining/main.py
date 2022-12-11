#!/usr/bin/env python
# coding: utf-8

# # Importing Libraries

# In[1]:


import pandas as pd
import numpy as np
import warnings
from sklearn.model_selection import cross_val_score, cross_val_predict, StratifiedKFold, train_test_split
from sklearn.metrics import f1_score
from sklearn.tree import DecisionTreeClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.neighbors import LocalOutlierFactor, KNeighborsClassifier
from sklearn.ensemble import IsolationForest, RandomForestClassifier, BaggingClassifier, VotingClassifier

warnings.filterwarnings("ignore")
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


# # Imputation

# In[2]:


def imputation_full(df, numerical_columns, nominal_columns):
    for col in df.columns:
        if(col in numerical_columns):
            df[col].fillna(df[col].mean(), inplace = True)
        elif(col in nominal_columns):
            df[col] = df[col].replace(np.nan, df[col].mode()[0])
        else:
            pass
    return df


# In[3]:


def imputation_class(df, numerical_columns, nominal_columns):
    
    class_values = list(df["Target (Col 107)"].unique())
    
    for col in df.columns:
        if(col in numerical_columns):
            for c in class_values:
                mean_value = df[df["Target (Col 107)"] == c][col].mean()
                df.loc[ ((df["Target (Col 107)"] == c) & (df[col].isnull())), col] = mean_value
        elif(col in nominal_columns):
            for c in class_values:
                mode_value = df[df["Target (Col 107)"] == c][col].mode()[0]
                df.loc[((df["Target (Col 107)"] == c) & (df[col].isnull())), col] = mode_value
        else:
            pass
    return df


# # Standardization

# In[4]:


def min_max(df, numerical_columns):
    df[numerical_columns] = df[numerical_columns].apply(lambda x: (x - x.min()) / (x.max() - x.min()))
    return df


# In[5]:


def z_score(df, numerical_columns):
    df[numerical_columns] = df[numerical_columns].apply(lambda x: (x - x.mean()) / x.std())
    return df


# # Outlier Detection

# In[6]:


def isolation_forest(df, numerical_columns, nominal_columns):
    model = IsolationForest(n_estimators=1000, max_samples='auto', contamination=float(0.2), random_state = 42)
    model.fit(df[numerical_columns + nominal_columns])
    
    df["anomaly_score"] = model.predict(df[numerical_columns + nominal_columns])
    df["anomaly_score"] = df["anomaly_score"].map({1:0, -1:1})
    df = df[df["anomaly_score"] == 0]
    df = df.drop("anomaly_score", axis=1)
    return df


# In[7]:


def statistical(df, numerical_columns, nominal_columns):
    df["anomaly_score"] = 0
    for col in numerical_columns:
        upper = df[col].mean() + (2*df[col].std())
        lower = df[col].mean() - (2*df[col].std())
        df.loc[((df[col] < lower) | (df[col] > upper)), "anomaly_score"] = 1

    df = df[df["anomaly_score"] == 0]
    df = df.drop("anomaly_score", axis=1)
    return df


# In[8]:


def lof(df, numerical_columns, nominal_columns):
    model = LocalOutlierFactor(n_neighbors = 100, novelty = False)
    
    df["anomaly_score"] = model.fit_predict(df[numerical_columns + nominal_columns])
    df["anomaly_score"] = df["anomaly_score"].map({1:0, -1:1})
    df = df[df["anomaly_score"] == 0]
    df = df.drop("anomaly_score", axis=1)
    return df


# # Feature Selection

# In[9]:


def feature_columns(df):
    df_col = df.corr().loc["Target (Col 107)"].to_frame()
    df_col["Target (Col 107)"] = df_col["Target (Col 107)"].abs()
    df_col = list(df_col[df_col["Target (Col 107)"] > 0.1].index)
    return df[df_col]


# In[10]:


def feature_no(df):
    return df


# # Model Classifiers

# In[11]:


def dtree(df):
    X = df.drop("Target (Col 107)", axis = 1)
    y = df["Target (Col 107)"]
    
    cv = StratifiedKFold(n_splits = 44, random_state = 42, shuffle = True)
    cv.get_n_splits(X, y)

    model = DecisionTreeClassifier(max_depth = 3, max_leaf_nodes = 5, random_state = 42)
    score = np.mean(cross_val_score(model, X, y, cv = cv, scoring = "f1"))
    
    return score


# In[12]:


def rforest(df):
    X = df.drop("Target (Col 107)", axis = 1)
    y = df["Target (Col 107)"]
    
    cv = StratifiedKFold(n_splits = 14, random_state = 42, shuffle = True)
    cv.get_n_splits(X, y)

    model = RandomForestClassifier(n_estimators = 165, n_jobs = -1, max_features = None, criterion = "entropy", random_state = 42)
    score = np.mean(cross_val_score(model, X, y, cv = cv, scoring = "f1"))
    
    return score


# In[13]:


def gnaivebayes(df):
    X = df.drop("Target (Col 107)", axis = 1)
    y = df["Target (Col 107)"]
    
    cv = StratifiedKFold(n_splits = 19, random_state = 42, shuffle = True)
    cv.get_n_splits(X, y)

    model = GaussianNB(var_smoothing = 1.0)
    score = np.mean(cross_val_score(model, X, y, cv = cv, scoring = "f1"))
    
    return score


# In[14]:


def knn(df):
    X = df.drop("Target (Col 107)", axis = 1)
    y = df["Target (Col 107)"]
    
    cv = StratifiedKFold(n_splits = 20, random_state = 42, shuffle = True)
    cv.get_n_splits(X, y)

    model = KNeighborsClassifier(n_neighbors = 3, metric = "manhattan")
    score = np.mean(cross_val_score(model, X, y, cv = cv, scoring = "f1"))
    
    return score


# In[15]:


def baggingensemble(df):
    X = df.drop("Target (Col 107)", axis = 1)
    y = df["Target (Col 107)"]
    
    cv = StratifiedKFold(n_splits = 20, random_state = 42, shuffle = True)
    cv.get_n_splits(X, y)
    
    model = BaggingClassifier(KNeighborsClassifier(n_neighbors = 3, metric = "manhattan"), n_estimators = 10,
                              random_state = 42, oob_score = True)
    
    score = np.mean(cross_val_score(model, X, y, cv = cv, scoring = "f1"))
    
    return score


# In[16]:


def voting(df):
    X = df.drop("Target (Col 107)", axis = 1)
    y = df["Target (Col 107)"]
    
    cv = StratifiedKFold(n_splits = 4, random_state = 42, shuffle = True)
    cv.get_n_splits(X, y)
    
    model1 = RandomForestClassifier(n_estimators = 165, n_jobs = -1, max_features = None, criterion = "entropy", random_state = 42)
    model2 = GaussianNB(var_smoothing = 1.0)
    model3 = KNeighborsClassifier(n_neighbors = 3, metric = "manhattan")
    
    model = VotingClassifier(estimators=[('rf', model1), ('nb', model2), ('knn', model3)], voting='hard')
    score = np.mean(cross_val_score(model, X, y, cv = cv, scoring = "f1"))
    
    return score


# # Main Function

# In[17]:


#Reading Data
main_df = pd.read_csv("./Ecoli.csv")

#Defining numerical columns, nominal columns and target values
numerical_columns = [i for i in main_df.columns if "Num" in i]
nominal_columns = [i for i in main_df.columns if "Nom" in i]

# Dropping columns that have a high percentage of nulls
high_null_col_df = (main_df.isnull().sum() * 100 / len(main_df)).to_frame()
high_null_col = list(high_null_col_df[high_null_col_df[0] > 10].index)
main_df = main_df.drop(high_null_col, axis=1)

#Results as DataFrame
result_df = pd.DataFrame(columns = ["f1", "imputation_method", "standardization", "outlier_detection", "feature_selection", "Classifier"])

#Performing pre-processing
for impute in [imputation_full, imputation_class]:
    impute_df = impute(main_df.copy(), numerical_columns, nominal_columns)
    
    for standardization in [min_max, z_score]:
        standard_df = standardization(impute_df.copy(), numerical_columns)
    
        for outlier in [isolation_forest, statistical, lof]:
            outlier_df = outlier(standard_df.copy(), numerical_columns, nominal_columns)
            
            for feature in [feature_columns, feature_no]:
                feature_df = feature(outlier_df.copy())
                
                for classifier in [dtree, rforest, gnaivebayes, knn, baggingensemble, voting]:
                    score_cv = classifier(feature_df.copy())
                    
                    row = {"f1" : score_cv, 
                          "imputation_method" : impute.__qualname__, 
                          "standardization" : standardization.__qualname__, 
                          "outlier_detection" : outlier.__qualname__, 
                          "feature_selection": feature.__qualname__, 
                          "Classifier" : classifier.__qualname__}
                    
                    result_df = result_df.append(row, ignore_index = True)

result_df.sort_values(by=["f1", "feature_selection", "standardization", "imputation_method"], 
                      ascending = [False, False, True, True], inplace = True)
result_df.reset_index(drop=True, inplace=True)

print("Top 10 classifiers methods and preprocessing techniques:")
print(result_df.head(10))


# In[18]:


#Make Predictions on test data, accuracy and f1 score on training data
result_dict = result_df.iloc[0].to_dict()
print("Best Techniques Identified:", result_dict)

final_df = locals()[ result_dict["imputation_method"] ](main_df, numerical_columns, nominal_columns)
final_df = locals()[ result_dict["standardization"] ](final_df, numerical_columns)
final_df = locals()[ result_dict["outlier_detection"] ](final_df, numerical_columns, nominal_columns)
final_df = locals()[ result_dict["feature_selection"] ](final_df)

X = final_df.drop("Target (Col 107)", axis = 1)
y = final_df["Target (Col 107)"]

cv = StratifiedKFold(n_splits = 20, random_state = 42, shuffle = True)
cv.get_n_splits(X, y)
model = KNeighborsClassifier(n_neighbors = 3, metric = "manhattan")
model.fit(X, y)

accuracy_sc = float(round(np.mean(cross_val_score(model, X, y, cv = cv, scoring = "accuracy")), 3))
f1_sc = float(round(np.mean(cross_val_score(model, X, y, cv = cv, scoring = "f1")), 3))
print("\nAccuracy calculated on Test Data", accuracy_sc, "\nF1 calculated on Test Data", f1_sc)


# In[19]:


#Outputing Final Predictions on test data
test_file = pd.read_csv("./Ecoli_test.csv")

output_df = pd.DataFrame(columns = ["0", "1"])
output_df["0"] = model.predict(test_file)
output_df["0"] = output_df["0"].astype(int)
output_df.fillna('', inplace = True)

output_df.to_csv("./s4761003.csv", header = False, index = False)
pd.DataFrame([[accuracy_sc, f1_sc]]).to_csv("./s4761003.csv", mode = "a", header = False, index = False)


# In[ ]:




