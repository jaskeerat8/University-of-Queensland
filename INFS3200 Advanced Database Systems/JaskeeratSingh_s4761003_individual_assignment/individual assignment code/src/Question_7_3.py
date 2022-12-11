import pandas as pd

def tokenize(string, q):
    if q != 0:
        if len(string) < q:
            str_tokens = [string]
        else:
            str_tokens = [string[i:i + q] for i in range(0, len(string) - q + 1, 1)]
        return list(set(str_tokens))
    else:
        str_tokens = string.split(" ")
        return list(set(str_tokens))

book1_columns = ["id", "title", "authors", "pubyear", "pubmonth", "pubday", "edition", "publisher",
"isbn13", "language", "series", "pages"]

book2_columns = ["id", "book_title", "authors", "publication_year", "publication_month",
"publication_day", "edition", "publisher_name", "isbn13", "language", "series", "pages"]

book1 = pd.read_csv("../data/Book1.csv", header = None, names = book1_columns)
book2 = pd.read_csv("../data/Book2.csv", header = None, names = book2_columns)

book1 = book1[["id", "title"]]
book2 = book2[["id", "book_title"]]

book1.fillna("", inplace=True)
book2.fillna("", inplace=True)

book1["title_tokenized"] = book1["title"].apply(tokenize, q = 3)
book2["title_tokenized"] = book2["book_title"].apply(tokenize, q = 3)


results = []
for i in range(len(book1)):
    id1 = book1["id"][i]
    str1_tokens = book1["title_tokenized"][i]

    for j in range(len(book2)):
        id2 = book2["id"][j]
        str2_tokens = book2["title_tokenized"][j]

        total_tokens = str1_tokens + str2_tokens
        total_tokens = list(set(total_tokens))
        sim = (len(str1_tokens) + len(str2_tokens) - len(total_tokens)) / len(total_tokens)
        if sim >= 0.75:
            results.append(str(id1) + ',' + str(id2))


try:
    with open('../data/Book1and2_pair.csv', 'r') as f:
        benchmark = f.read().splitlines()
    if benchmark is None:
        print("no file.")
except:
    print("Error occurred. Check if file exists")

pair_found = []
count = 0
for pair in results:
    if pair in benchmark:
        pair_found.append(pair)
        count = count + 1

print("Student ID: s4761003")
if count == 0:
    print('Precision=0, Recall=0, Fmeasure=0')
else:
    precision = count / len(results)
    recall = count / len(benchmark)
    f_measure = 2 * precision * recall / (precision + recall)

    print("Pairs found in gold-standard dataset: " + str(pair_found))
    print(f'Precision= {precision}, Recall= {recall}, Fmeasure= {f_measure}')