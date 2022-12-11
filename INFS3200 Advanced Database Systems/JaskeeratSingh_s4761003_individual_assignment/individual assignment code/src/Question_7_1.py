s1 = "Peter Rob, Carlos Coronel"
s2 = "Carlos Coronel;Steven Morris;Peter Rob;"

if (len(s1) == 0):
    edit_distance = len(s2)
elif (len(s2) == 0):
    edit_distance = len(s1)
else:
    len_source = len(s1) + 1
    len_target = len(s2) + 1

    arr = []
    for i in range(len_target):
        arr.append([0] * len_source)

    for i in range(len_target):
        for j in range(len_source):
            if (i == 0):
                arr[i][j] = j
            elif (j == 0):
                arr[i][j] = i
            elif (s2[i - 1] == s1[j - 1]):
                arr[i][j] = arr[i - 1][j - 1]
            else:
                arr[i][j] = 1 + min(arr[i][j - 1], arr[i - 1][j], arr[i - 1][j - 1])
    edit_distance = arr[len_target - 1][len_source - 1]

print("Student ID: s4761003")
print("Edit Distance:", edit_distance)