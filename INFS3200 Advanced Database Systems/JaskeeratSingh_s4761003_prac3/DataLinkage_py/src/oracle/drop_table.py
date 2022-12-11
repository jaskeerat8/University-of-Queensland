'''
Created on 1 May 2020
Modified on 2 May 2020
@author: shree
'''

import src.oracle.DBconnect as db

try:
    drop = ("DROP TABLE RESTAURANT")

    # print(create)

    con = db.create_connection()
    cur = db.create_cursor(con)

    cur.execute(drop)
    print("Table dropped successfully.")
    cur.close()
    con.close()
except:
    print("Error occurred. Table drop failed.")
