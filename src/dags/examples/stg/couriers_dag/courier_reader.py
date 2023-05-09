from datetime import datetime
from typing import Dict, List
import requests
import json



class CourierReader:
    def __init__(self) -> None:
        self.docs = "couriers"
        self.data=[]
 
    def get_couriers(self,  limit, offset) -> List[Dict]:
        headers = {
    		'X-Nickname': 'a_wolkov',
    		'X-Cohort': '12',
    		'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f',}

        params = {
	    'restaurant_id': '',
	    'from': '',
	    'to': '',
	    'sort_field': 'courier_id',
	    'sort_direction': 'asc',
	    'limit': limit,
	    'offset':  offset}
	
        url="https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/"+self.docs
 
        increment=requests.get(url, headers=headers, params=params)
         
        if increment.json()!=[]:
        	offset+=50
        	self.data.extend(increment.json()) 
        	self.get_couriers(50, offset)
            
        else:

            return 0
        return self.data
