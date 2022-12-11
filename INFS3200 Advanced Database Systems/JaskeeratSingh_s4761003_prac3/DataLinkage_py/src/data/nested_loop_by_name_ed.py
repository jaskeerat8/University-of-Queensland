'''
Created on 1 May 2020

@author: shree
'''

import src.oracle.DBconnect as db
import src.data.similarity as similarity
import src.data.csv_loader as csv
import src.data.measurement as measure
import numpy as np
from src.data.restaurant import restaurant as res
import datetime


# def nested_loop_by_name_ed(x):
#     threshold = x

def nested_loop_by_name_ed():

    threshold = 0.8

    con=db.create_connection()
    cur=db.create_cursor(con)
    string_query = "SELECT * FROM RESTAURANT";
    cur.execute(string_query);
    restaurants=[];
    for rid ,name, address, city  in cur:
        restaurant = res()
        restaurant.set_id(rid);
        restaurant.set_name(name);
        restaurant.set_address(address);
        restaurant.set_city(city);
        restaurants.append(restaurant)
        
    cur.close();
    con.close();


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
            sim = similarity.calc_ed_sim(name1, name2)
            if sim >= threshold:
                results.append(str(id1) + '_' + str(id2))

    end_time = datetime.datetime.now()
    time = end_time - start_time
    #print("Total Time:", round(time.total_seconds() * 1000, 3), 'milliseconds')
    measure.load_benchmark()
    #print("Student_id: s4761003")
    #print("Threshold: " + str(threshold))
    #precision, recall, f_measure = measure.calc_measure(results)
    #return precision, recall, f_measure
    measure.calc_measure(results)


nested_loop_by_name_ed()

# print("Student_id: s4761003")
# for i in np.arange(0.1, 1.1, 0.1):
#     precision, recall, f_measure = nested_loop_by_name_ed(round(i, 1))
#     print("Threshold=", str(round(i, 1)), "Precision=", precision, ", Recall=", recall, ", Fmeasure=", f_measure)
