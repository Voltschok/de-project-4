from datetime import datetime
from typing import Any
from lib.dict_util import json2str
from psycopg import Connection


class PgSaverDeliveries:


    def save_object(self, conn: Connection, id: str,  delivery_ts: datetime, val: Any):
        str_val = json2str(val)
        #print(type(str_val), str_val)
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliveries( delivery_id, delivery, delivery_ts)
                    VALUES ( %(delivery_id)s, %(delivery)s, %(delivery_ts)s  )
                    ON CONFLICT (delivery_id) DO UPDATE
                    SET
                        delivery = EXCLUDED.delivery,
                        delivery_ts=EXCLUDED.delivery_ts;
                """,
                {
                    "delivery_id": id,
                    "delivery": str_val,
                    "delivery_ts": delivery_ts    
                }
            )

