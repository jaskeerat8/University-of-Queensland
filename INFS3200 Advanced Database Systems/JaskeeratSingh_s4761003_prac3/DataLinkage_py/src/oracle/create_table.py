'''
Created on 1 May 2020
Modified on 2 May 2020
@author: shree
'''

import src.oracle.DBconnect as db

try:
    create = ("CREATE TABLE RESTAURANT("
              "ID NUMBER NOT NULL,"
              "NAME VARCHAR2(100 ) NOT NULL,"
              "ADDRESS VARCHAR2(255 ) NOT NULL,"
              "CITY VARCHAR2(50 ) NOT NULL,"
              "CONSTRAINT RESTAURANT_PK PRIMARY KEY(ID) ENABLE)"
              )

    # print(create)

    con = db.create_connection()
    cur = db.create_cursor(con)

    cur.execute(create)
    print("Table created successfully.")
    cur.close()
    con.close()
except:
    print("Error occurred. Table Creation failed.")
