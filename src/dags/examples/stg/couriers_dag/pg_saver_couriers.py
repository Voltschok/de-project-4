from datetime import datetime
from typing import Any
from lib.dict_util import json2str
from psycopg import Connection


class PgSaverCouriers:


    def save_object(self, conn: Connection, id: str,   val: Any):
        str_val = json2str(val)
        #print(type(str_val), str_val)
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.couriers( courier_id, courier, update_ts)
                    VALUES ( %(courier_id)s, %(courier)s, current_timestamp  )
                    ON CONFLICT (courier_id) DO UPDATE
                    SET
                        courier = EXCLUDED.courier;
                """,
                {
                    "courier_id": id,
                    "courier": str_val  
                }
            )

