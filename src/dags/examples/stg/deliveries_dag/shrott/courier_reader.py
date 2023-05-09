from datetime import datetime
from typing import Dict, List

from lib import ApiConnect

        
headers = {
    'X-Nickname': 'a_wolkov',
    'X-Cohort': '12',
    'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f',
}

params =     {'restaurant_id': '',
    'from': '',
    'to': '',
    'sort_field': 'name',
    'sort_direction': 'asc',
    'limit': '20',
    'offset': '',
	}

class CourierReader:
    def __init__(self, ac: ApiConnect) -> None:
        self.dbs = ac.client()
 
	couriers = requests.get('https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers', headers=headers, params=params)
 
	return 

 
