from datetime import datetime, timedelta, date
from typing import Dict, List
import requests
import json



class DeliveryReader:
    def __init__(self) -> None:
        self.docs = "deliveries"
        self.docs2=[]

    def get_deliveries(self,  limit, last_loaded_id, offset) -> List[Dict]:
        headers = {
    		'X-Nickname': 'a_wolkov',
    		'X-Cohort': '12',
    		'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f',}

        
        url="https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/"+self.docs
         
 
        params = {
	    'restaurant_id': '',
	    'from': '',
	    'to': (date.today()-timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S'),
	    'sort_field': 'delivery_ts',
	    'sort_direction': 'asc',
	    'limit': limit,
	    'offset': offset }
	 
        increment=requests.get(url, headers=headers, params=params)
        if increment.json()!=[]:
        	print(last_loaded_id)
        	if datetime.fromisoformat((increment.json())[-1]["delivery_ts"])> last_loaded_id:
        		offset+=50
        		self.docs2.extend(increment.json())
        		self.get_deliveries( 50, datetime.fromisoformat((increment.json())[-1]["delivery_ts"]), offset)
        	else:
        		return 0
            #print(increment.json(), offset)
            		
            
            
        else:
        
        
            
            #print(type(self.docs2))
            #print(self.docs2)
            return 0
        return self.docs2
        
           
        
