'''
Created on 1 May 2020

@author: shree
'''


def calc_jaccard(str1, str2, q):
    str1_tokens = tokenize(str1, q)
    str2_tokens = tokenize(str2, q)
    total_tokens = str1_tokens + str2_tokens
    total_tokens = list(set(total_tokens))
    return (len(str1_tokens) + len(str2_tokens) - len(total_tokens)) / len(total_tokens)


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


def calc_ed_sim(str1, str2):
    if str1 == str2:
        return 1
    ed = calc_ed(str1, str2)

    return 1 - (ed / max(len(str1), len(str2)))


def calc_ed(str1, str2):
    ed = 0

    if (len(str1) == 0):
        ed = len(str2)
    elif (len(str2) == 0):
        ed = len(str1)
    else:
        len_source = len(str1) + 1
        len_target = len(str2) + 1

        arr = []
        for i in range(len_target):
            arr.append([0] * len_source)

        for i in range(len_target):
            for j in range(len_source):
                if (i == 0):
                    arr[i][j] = j
                elif (j == 0):
                    arr[i][j] = i
                elif (str2[i - 1] == str1[j - 1]):
                    arr[i][j] = arr[i - 1][j - 1]
                else:
                    arr[i][j] = 1 + min(arr[i][j - 1], arr[i - 1][j], arr[i - 1][j - 1])
        ed = arr[len_target - 1][len_source - 1]

    return ed