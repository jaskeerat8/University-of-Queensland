'''
Created on 1 May 2020

@author: shree
'''


class restaurant:
    '''
    classdocs
    '''

    def __init__(self, id=None, name=None, address=None, city=None):
        '''
        Constructor
        '''
        self.id = id
        self.name = name
        self.address = address
        self.city = city

    def get_id(self):
        return self.__id

    def get_name(self):
        return self.__name

    def get_address(self):
        return self.__address

    def get_city(self):
        return self.__city

    def set_id(self, value):
        self.__id = value

    def set_name(self, value):
        self.__name = value

    def set_address(self, value):
        self.__address = value

    def set_city(self, value):
        self.__city = value
