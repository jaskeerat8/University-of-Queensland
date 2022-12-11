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


s1 = "Peter Rob, Carlos Coronel"
s2 = "Carlos Coronel;Steven Morris;Peter Rob;"

print("Student ID: s4761003")
for q in range(1,6):
    str1_tokens = tokenize(s1, q)
    str2_tokens = tokenize(s2, q)
    total_tokens = str1_tokens + str2_tokens
    total_tokens = list(set(total_tokens))
    jaccard_distance =  1 - (len(str1_tokens) + len(str2_tokens) - len(total_tokens)) / len(total_tokens)
    print(f"Jaccard Distance with tokenization parameter set as {str(q)}:", jaccard_distance)


