from logging import Logger
from typing import List
from datetime import datetime
from examples.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DmDeliveryObj(BaseModel):
    id: int
    order_id: str
    order_ts: datetime
    courier_id: str
    delivery_id: str  
    address: str
    delivery_ts: datetime
    rate: int
    sum: float
    tip_sum: float
 

class DmDeliveryOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def dm_list_deliveries(self, delivery_threshold: int, limit: int) -> List[DmDeliveryObj]:
        with self._db.client().cursor(row_factory=class_row(DmDeliveryObj)) as cur:
            cur.execute(
                """

			SELECT
                        id,
                        delivery::json->>'order_id' as order_id,
                        (delivery::json->>'order_ts')::timestamp as order_ts,
                        delivery::json->>'courier_id' as courier_id,
                        delivery::json->>'delivery_id' as delivery_id,
                        delivery::json->>'address' as address,
                        (delivery::json->>'delivery_ts')::timestamp  as delivery_ts,
                        (delivery::json->>'rate')::int as rate,
                        (delivery::json->>'sum')::numeric(14,2) as sum,
                        (delivery::json->>'tip_sum')::numeric(14,2) as tip_sum

			FROM stg.deliveries d 

                    	WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    	ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    	LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                   
                """, {
                    "threshold": delivery_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
            print(objs)
        return objs


class DmDeliveryDestRepository:

    def insert_delivery(self, conn: Connection, delivery: DmDeliveryObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
					INSERT INTO dds.dm_deliveries (id, order_id, order_ts, 
			            courier_id, delivery_id,  address, delivery_ts, 
                        rate, sum, tip_sum)                  
            		VALUES (%(id)s, %(order_id)s  ,   %(order_ts)s , %(courier_id)s,
                   	 %(delivery_id)s,%(address)s, %(delivery_ts)s, %(rate)s,   %(sum)s, %(tip_sum)s )
            		ON CONFLICT (order_id )
					DO UPDATE SET 
			
                    order_ts=EXCLUDED.order_ts,
                    courier_id=EXCLUDED.courier_id,
                    delivery_id=EXCLUDED.delivery_id,
                    address=EXCLUDED.address,
                    delivery_ts=EXCLUDED.delivery_ts,
                    rate=EXCLUDED.rate,
                    sum=EXCLUDED.sum,
                    tip_sum=EXCLUDED.tip_sum; 
					""",
                {
                    
                    "id": delivery.id,
                    "order_id" :delivery.order_id,
		            "order_ts": delivery.order_ts,
                    "courier_id": delivery.courier_id,
                    "delivery_id": delivery.delivery_id,
                    "address": delivery.address,
                    "delivery_ts": delivery.delivery_ts,
                    "rate": delivery.rate,
                    "sum": delivery.sum,
                    "tip_sum": delivery.tip_sum
                },
            )


class DmDeliveryLoader:
    WF_KEY = "example_deliveries_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmDeliveryOriginRepository(pg_origin)
        self.dds = DmDeliveryDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def dm_load_deliveries(self):
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
            load_queue = self.origin.dm_list_deliveries(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} dm_deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for dm_delivery in load_queue:
                self.dds.insert_delivery(conn, dm_delivery)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
