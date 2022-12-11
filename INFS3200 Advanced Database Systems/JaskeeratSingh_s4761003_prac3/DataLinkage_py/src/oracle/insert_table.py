'''
Created on 1 May 2020
Modified on 2 May 2020
@author: shree
'''

import src.oracle.DBconnect as db

try:
    con = db.create_connection()
    cur = db.create_cursor(con)

    file1 = open('../../data/restaurant.csv', 'r')
    Lines = file1.readlines()
    for line in Lines:
        line = line.replace("'", " ")
        line = line.split(',')
        insert = "INSERT INTO RESTAURANT VALUES (" + line[0] + ", '" + line[1] + "', '" + line[2] + "', '" + line[
            3] + "')"
        # print(insert)
        cur.execute(insert)

    cur.execute('commit')
    print("Statement executed successfully.")
    cur.close()
    con.close()
except:
    print("Error occurred. Statement execution failed.")
