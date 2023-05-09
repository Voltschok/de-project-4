from logging import Logger
from typing import List
from datetime import datetime
from examples.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DmProductObj(BaseModel):
    #id:int
    product_id: str
    product_name: str
    product_price: float
    active_from: datetime
    active_to: datetime	
    restaurant_id: int

class DmProductsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def dm_list_products(self, product_threshold: int, limit: int) -> List[DmProductObj]:
        with self._db.client().cursor(row_factory=class_row(DmProductObj)) as cur:
            
            cur.execute(
                """

                 

 					WITH temp0 AS
 
                        (SELECT   
                            json_array_elements(oo.object_value::json->'order_items')::json->>'id' as product_id,
                            json_array_elements(oo.object_value::json->'order_items')::json->>'name' as product_name,
                            (json_array_elements(oo.object_value::json->'order_items')::json->>'price')::numeric(14,2) as product_price,
                            oo.update_ts as active_from,
                            '2099-12-31'::timestamp as active_to,
                            dr.id as restaurant_id

                        FROM stg.ordersystem_orders oo   
                        LEFT JOIN dds.dm_restaurants dr ON dr.restaurant_id=(oo.object_value::json->'restaurant')->>'id')  
                    SELECT  product_id, product_name, product_price, 
                    min(active_from) as active_from, max(active_to) as active_to, restaurant_id from temp0
                    GROUP BY product_id, product_name, product_price, restaurant_id
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                    
                """, {
                    "threshold": product_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
           
            
            print(objs)
        return objs


class DmProductDestRepository:

    def insert_product(self, conn: Connection, product: DmProductObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                	INSERT INTO dds.dm_products ( product_id,product_name, product_price, active_from, active_to, restaurant_id)
                  
                    VALUES (   %(product_id)s,  %(product_name)s  ,   %(product_price)s,    %(active_from)s, 
                     %(active_to)s ,  %(restaurant_id)s   )
                    ON CONFLICT (product_id) DO UPDATE
                    SET
                         
                        product_name=EXCLUDED.product_name,
                        product_price=EXCLUDED.product_price,
                        active_from=EXCLUDED.active_from,
                        active_to=EXCLUDED.active_to,
                        restaurant_id=EXCLUDED.restaurant_id;
 			
 			
 			;
                """,
                {
                    "product_id": product.product_id,
                    "product_name": product.product_name,
                    "product_price": product.product_price,
                    "active_from": product.active_from,
                    "active_to": product.active_to,
                    "restaurant_id": product.restaurant_id
                },
            )


class DmProductLoader:
    WF_KEY = "example_products_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmProductsOriginRepository(pg_origin)
        self.dds = DmProductDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def dm_load_products(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
             
            	wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.dm_list_products(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} dm_products to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for dm_product in load_queue:
                self.dds.insert_product(conn, dm_product)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.product_id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
