'''
Created on 1 May 2020
Modified on 2 May 2020
@author: shree
'''

import src.oracle.DBconnect as db
from src.data.restaurant import restaurant as res

try:
    con = db.create_connection()
    cur = db.create_cursor(con)
    string_query = "SELECT * FROM RESTAURANT"
    restaurants = []
    cur.execute(string_query)
    for rid, name, address, city in cur:
        restaurant = res()
        restaurant.set_id(rid)
        restaurant.set_name(name)
        restaurant.set_address(address)
        restaurant.set_city(city)
        restaurants.append(restaurant)
        print(restaurant.get_id(), ',', restaurant.get_name(), ',', restaurant.get_address(), ',',
              restaurant.get_city())
    cur.close()
    con.close()
    print("Statement executed successfully.", len(restaurants), " records in total.")
except:
    print("Error occurred. Statement execution failed.")
