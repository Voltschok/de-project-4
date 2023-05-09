from logging import Logger
from typing import List
from datetime import datetime
from examples.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DmOrderObj(BaseModel):
    id: int
    order_key: str
    order_status: str
    restaurant_id: str
    timestamp_id: int
    user_id: int

class DmOrdersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def dm_list_orders(self, order_threshold: int, limit: int) -> List[DmOrderObj]:
        with self._db.client().cursor(row_factory=class_row(DmOrderObj)) as cur:
            cur.execute(
                """
                        SELECT oo.id as id,
                                oo.object_id as order_key,
                                object_value::json->>'final_status'  as order_status,
                                dr.id as restaurant_id,
                                t.id as timestamp_id,
                                du.id as user_id
                                FROM stg.ordersystem_orders oo

                     
                     
                        LEFT JOIN dds.dm_timestamps t ON oo.id=t.id
                        LEFT JOIN dds.dm_users du ON du.user_id=(oo.object_value::JSON->'user')::json->>'id'
                        LEFT JOIN dds.dm_restaurants dr ON dr.restaurant_id=(oo.object_value::json->'restaurant')->>'id' 

                        WHERE oo.id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                        ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                        LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                   
                """, {
                    "threshold": order_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
            print(objs)
        return objs


class DmOrderDestRepository:

    def insert_order(self, conn: Connection, order: DmOrderObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                	INSERT INTO dds.dm_orders (id, order_key, order_status, restaurant_id, timestamp_id, user_id)
                  
                    VALUES (%(id)s, %(order_key)s  ,   %(order_status)s , 
                    %(restaurant_id)s, %(timestamp_id)s,  %(user_id)s )
                    ON CONFLICT (id) DO UPDATE
                    SET
                         
                        order_key=EXCLUDED.order_key,
                        order_status=EXCLUDED.order_status,
                        restaurant_id=EXCLUDED.restaurant_id,
                        timestamp_id=EXCLUDED.timestamp_id,
                        user_id=EXCLUDED.user_id;
                """,
                {
                    
                        "id": order.id,
                        "order_key": order.order_key,
                        "order_status": order.order_status,
                        "restaurant_id": order.restaurant_id,
                        "timestamp_id": order.timestamp_id,
                        "user_id": order.user_id
                },
            )


class DmOrderLoader:
    WF_KEY = "example_orders_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmOrdersOriginRepository(pg_origin)
        self.dds = DmOrderDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def dm_load_orders(self):
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
            load_queue = self.origin.dm_list_orders(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} dm_orders to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for dm_order in load_queue:
                self.dds.insert_order(conn, dm_order)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
