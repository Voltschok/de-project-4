from datetime import datetime
from typing import Any

# from lib.dict_util import json2str
# from psycopg import Connection


class PgSaver_couriers:

    def save_object(self, conn: Connection, update_ts: datetime, val: Any):
    	 
        str_val = json2str(val)
        with conn.cursor() as cur:
		
            cur.execute(
                """
                    INSERT INTO stg.couriers( courier, update_ts)
                    VALUES ( %(courier)s , current_timestamp )
                    
                """,
                {
                    
                    "courier": str_val  
                }
            )