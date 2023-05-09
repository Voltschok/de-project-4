from logging import Logger
from typing import List
from datetime import datetime
from examples.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DmSaleObj(BaseModel):
    #id: int
    product_id: str
    order_id: str
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float

class DmSalesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def dm_list_sales(self, sale_threshold: int, limit: int) -> List[DmSaleObj]:
        with self._db.client().cursor(row_factory=class_row(DmSaleObj)) as cur:
            cur.execute(
                """
                    WITH temp1 as ( 
			        SELECT 
                    do2.id as order_id,
                    oo.object_id as  id,
                    (json_array_elements(oo.object_value::json->'order_items')::json->>'id')::varchar  as product_id,
                    (json_array_elements(oo.object_value::json->'order_items')::json->>'quantity')::numeric  as count,
                    (json_array_elements(oo.object_value::json->'order_items')::json->>'price')::numeric  as price,
                    (json_array_elements(oo.object_value::json->'order_items')::json->>'quantity')::numeric  as quantity,
                    (json_array_elements(oo.object_value::JSON->'order_items')::json->>'price')::numeric as total_sum

		            FROM stg.ordersystem_orders oo 
		            LEFT JOIN dds.dm_orders do2 ON do2.id= oo.id
		            WHERE do2.order_status ='CLOSED'),
		            temp2 as ( 
		            SELECT 
                        (event_value ::json->>'order_id')::varchar as order_id ,
                        (json_array_elements(be.event_value::json->'product_payments')::json->>'bonus_payment')::numeric as bonus_payment,
                        (json_array_elements(be.event_value::json->'product_payments')::json->>'bonus_grant')::numeric as bonus_grant,
                        (json_array_elements(be.event_value::json->'product_payments')::json->>'product_id')::varchar as product_id


		 
                    FROM stg.bonussystem_events be where event_type='bonus_transaction')
                    SELECT temp1.order_id, dp.id as product_id, temp1.count, temp1.price, temp1.price*temp1.quantity as total_sum, temp2.bonus_payment, temp2.bonus_grant
                    FROM temp1
                    JOIN temp2 ON temp1.id=temp2.order_id AND temp1.product_id=temp2.product_id
                    JOIN dds.dm_products dp ON temp1.product_id=dp.product_id
  
		
		
                    --WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    --ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                   
                """, {
                    "threshold": sale_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
            print(objs)
        return objs


class DmSaleDestRepository:

    def insert_sale(self, conn: Connection, sale: DmSaleObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                	INSERT INTO dds.fct_product_sales  ( product_id, order_id, count, price, total_sum, 
                	bonus_payment, bonus_grant)
                  
                    VALUES (  %(product_id)s  ,   %(order_id)s , %(count)s,  %(price)s,
                    %(total_sum)s,  %(bonus_payment)s, %(bonus_grant)s  )
                    ON CONFLICT (order_id, product_id) DO UPDATE
                    SET
    
                    count=EXCLUDED.count,
                    price=EXCLUDED.price,
                    total_sum=EXCLUDED.total_sum,
                    bonus_payment=EXCLUDED.bonus_payment,
                    bonus_grant=EXCLUDED.bonus_grant
                """,
                {
                    
                     
                    "product_id": sale.product_id,
                    "order_id": sale.order_id,
                    "count": sale.count,
                    "price": sale.price,
                    "total_sum": sale.total_sum,
                    "bonus_payment": sale.bonus_payment,
                    "bonus_grant": sale.bonus_grant
                },
            )


class DmSaleLoader:
    WF_KEY = "example_sales_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmSalesOriginRepository(pg_origin)
        self.dds = DmSaleDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def dm_load_sales(self):
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
            load_queue = self.origin.dm_list_sales(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} dm_sales to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for dm_sale in load_queue:
                self.dds.insert_sale(conn, dm_sale)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            # wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.product_id for t in load_queue])
            # wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            # self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
