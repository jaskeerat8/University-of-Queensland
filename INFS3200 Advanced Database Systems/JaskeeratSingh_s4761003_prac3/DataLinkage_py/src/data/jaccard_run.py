import src.data.nested_loop_by_name_jaccard as jaccard
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")

df = pd.DataFrame(columns=["q", "threshold", "precision", "recall", "f1"])

q_range = range(1,6)
threshold_range = np.arange(0.2, 1.2, 0.2)

for q in q_range:
    for threshold in threshold_range:
        precision, recall, f_measure = jaccard.nested_loop_by_name_jaccard(q, threshold)
        df = df.append({"q":int(q), "threshold":threshold, "precision":precision, "recall":recall, "f1":f_measure}, ignore_index=True)

df.sort_values(by=["threshold", "q"], inplace=True)
df.reset_index(drop=True, inplace=True)
df["q"] = df["q"].astype('int64')

print("Student_id: s4761003")
print(df)