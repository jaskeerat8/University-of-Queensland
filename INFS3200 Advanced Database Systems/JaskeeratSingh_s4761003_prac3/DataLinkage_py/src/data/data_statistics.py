import src.oracle.DBconnect as db
from src.data.restaurant import restaurant as res
import datetime
import src.data.csv_loader as csv
import src.data.measurement as measure

def task1():

    con = db.create_connection()
    cur = db.create_cursor(con)
    string_query = """
    select substr(city, 1, length(city)-1), count(*) as count 
    from restaurant 
    where substr(city, 1, length(city)-1) in ('la', 'los angeles') 
    group by city"""
    cur.execute(string_query)

    city_restaurants = cur.fetchall()
    la = city_restaurants[0][1]
    las_vegas = city_restaurants[1][1]

    string_query = """
    select count(distinct replace(city, CHR(10), '')) 
    from restaurant"""
    cur.execute(string_query)
    distinct_cities = cur.fetchall()[0][0]

    return(la, las_vegas, distinct_cities)

response = task1()
print("Student_id: s4761003")
print("La: " + str(response[0]))
print("los angeles: " + str(response[1]))
print("Number of Distinct Values in City: " + str(response[2]))