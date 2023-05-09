from logging import Logger
from typing import List
from datetime import datetime
from examples.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class FctDeliveryObj(BaseModel):
    id: int
    courier_id: str
    delivery_id: str
    delivery_ts: datetime
    order_id: str
    rate: int
    total_sum: float
    tip_sum: float

 

class FctDeliveriesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_fct_deliveries(self, delivery_threshold: int, limit: int) -> List[FctDeliveryObj]:
        with self._db.client().cursor(row_factory=class_row(FctDeliveryObj)) as cur:
            cur.execute(
                """
			        SELECT  dmd.id as id,
			                ddc.courier_id,

			                dmd.delivery_id, 
			                dmd.delivery_ts,
			                dmd.order_id,

			                dmd.rate as rate,
			                dmd.sum as total_sum,
			                dmd.tip_sum as  tip_sum 		 
			 			
					FROM dds.dm_deliveries dmd
					
			        LEFT JOIN dds.dm_couriers ddc on ddc.courier_id =dmd.courier_id 
			        LEFT JOIN dds.dm_orders dmo on dmo.order_key=dmd.order_id 
		 
                    WHERE dmd.id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
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


class FctDeliveryDestRepository:

    def insert_fct_delivery(self, conn: Connection, delivery: FctDeliveryObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                	INSERT INTO dds.fct_deliveries  (id, 
                	
                	courier_id, delivery_id, delivery_ts, order_id, rate, total_sum, 
                	tip_sum)
                  
                    VALUES (%(id)s, %(courier_id)s  ,   %(delivery_id)s , %(delivery_ts)s, %(order_id)s,  %(rate)s,
                    %(total_sum)s,  %(tip_sum)s  )
                    ON CONFLICT (order_id) DO UPDATE
                    SET
                         
                        courier_id=EXCLUDED.courier_id,
                        delivery_id=EXCLUDED.delivery_id,
                        delivery_ts=EXCLUDED.delivery_ts,
                        --order_id=EXCLUDED.order_id,
                        rate=EXCLUDED.rate,

                        
                        total_sum=EXCLUDED.total_sum,
                        tip_sum=EXCLUDED.tip_sum

                """,
                {
                        
                        "id": delivery.id,
                        "courier_id": delivery.courier_id,
                        "delivery_id": delivery.delivery_id,
                        "delivery_ts": delivery.delivery_ts,
                        "order_id": delivery.order_id,
                        
                        "rate": delivery.rate,
                        "total_sum": delivery.total_sum,
                        "tip_sum": delivery.tip_sum

 
                },
            )


class FctDeliveryLoader:
    WF_KEY = "example_fct_deliverys_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = FctDeliveriesOriginRepository(pg_origin)
        self.dds = FctDeliveryDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def fct_load_deliveries(self):
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
            load_queue = self.origin.list_fct_deliveries(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} fct_deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for fct_delivery in load_queue:
                self.dds.insert_fct_delivery(conn, fct_delivery)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
