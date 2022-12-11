'''
Created on 1 May 2020

@author: shree
'''

from src.data.restaurant import restaurant as res


def csv_loader():
    try:

        with open('../../data/restaurant.csv', 'r') as f:
            lines = f.read().splitlines()
        if lines is None:
            print("no file.")
            return
        restaurants = []

        for line in lines:
            line = line.replace("'", " ")
            line = line.split(',')
            restaurant = res()
            restaurant.set_id(line[0])
            restaurant.set_name(line[1])
            restaurant.set_address(line[2])
            restaurant.set_city(line[3])
            restaurants.append(restaurant)
            # print(restaurant.get_id(),',',restaurant.get_name(),',',restaurant.get_address(),',',restaurant.get_city())
        return restaurants

    except:
        print("Error occurred. Check if file exists.")

# res_list1 = csv_loader()
# print(res_list1[0].get_name())
