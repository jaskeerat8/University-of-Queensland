import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")

book3_columns = ["ID", "Title", "Author1", "Author2", "Author3", "Publisher", "ISBN13", "Date",
"Pages", "ProductDimensions", "SalesRank", "RatingsCount", "RatingValue",
"PaperbackPrice", "HardcoverPrice", "EbookPrice", "AudiobookPrice"]

book3 = pd.read_csv("../data/Book3.csv", header = None, names = book3_columns)
book3_sample = pd.DataFrame()

for i in book3.index:
    if(book3["ID"][i] % 100 == 0):
        book3_sample = book3_sample.append(book3.iloc[i], ignore_index=True)

print("Student ID: s4761003")
print("Number of Records in the Sample dataset:", book3_sample.shape[0])

book3_sample_nulls = book3_sample.isna().sum().to_frame()
print("Number of columns with nulls:", len(book3_sample_nulls[book3_sample_nulls[0] > 0]))

number_of_nulls = book3_sample_nulls[0].sum()
print("Number of Nulls in the dataset:", number_of_nulls)

DPMO = (number_of_nulls / (book3_sample.shape[0] * book3_sample.shape[1])) * (10**6)
print("Defects per Million Opportunities:", DPMO)