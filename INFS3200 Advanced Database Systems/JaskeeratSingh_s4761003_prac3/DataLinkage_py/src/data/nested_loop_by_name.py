'''

Created on 1 May 2020

@author: shree
'''

import src.oracle.DBconnect as db
from src.data.restaurant import restaurant as res
import datetime
import src.data.csv_loader as csv
import src.data.measurement as measure


def nested_loop_by_name():

    # option1: read data from database

    con = db.create_connection()
    cur = db.create_cursor(con)
    string_query = "SELECT * FROM RESTAURANT"
    cur.execute(string_query)
    restaurants = []
    for rid, name, address, city in cur:
        restaurant = res()
        restaurant.set_id(rid)
        restaurant.set_name(name)
        restaurant.set_address(address)
        restaurant.set_city(city)
        restaurants.append(restaurant)

    cur.close()
    con.close()


    # option2: read data from csv file, switch to this option by commenting the above code and uncommenting next line
    #restaurants = csv.csv_loader()
    results = []
    restaurant1 = res()
    restaurant2 = res()
    id1 = 0
    id2 = 0
    name1 = None
    name2 = None
    start_time = datetime.datetime.now()
    for i in range(0, len(restaurants)):
        restaurant1 = restaurants[i]
        id1 = restaurant1.get_id()
        name1 = restaurant1.get_name()
        for j in range(i + 1, len(restaurants)):
            restaurant2 = restaurants[j]
            id2 = restaurant2.get_id()
            name2 = restaurant2.get_name()
            if (name1 == name2):
                results.append(str(id1) + '_' + str(id2))

    endtime = datetime.datetime.now()
    time = endtime - start_time
    print("Total Time:", round(time.total_seconds() * 1000, 3), 'milliseconds')
    # print(results)
    measure.load_benchmark()
    measure.calc_measure(results)


# End of function

nested_loop_by_name()
