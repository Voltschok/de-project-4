from logging import Logger
from typing import List
from datetime import datetime 
from examples.cdm.cdm_settings_repository import EtlSetting, CdmEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
import json

class CourierLedgerObj(BaseModel):
	 
	courier_id: str
	courier_name: str
	settlement_year: int
	settlement_month: int
	orders_count: int
	orders_total_sum: float
	rate_avg: int
	order_processing_fee: float
	courier_order_sum: float
	courier_tips_sum: float
	courier_reward_sum: float

 
class CourierLedgerOriginRepository:
    def __init__(self, pg: PgConnect) -> None:

        self._db = pg
            
	
    def cdm_courier_ledger(self, threshold, limit: int) -> List[CourierLedgerObj]:
        with self._db.client().cursor(row_factory=class_row(CourierLedgerObj)) as cur:

            print(f'Treshold type: {type(threshold["year"])}, {type(threshold["month"])}')
            cur.execute(
                """
							WITH temp1  as ( 
                        	SELECT
			  
							fd.courier_id ,
							ddc.courier_name,
 							EXTRACT(YEAR from (delivery_ts)::timestamp) as settlement_year,
							EXTRACT(MONTH from (delivery_ts)::timestamp) as settlement_month,
							count(distinct dmo.id) as orders_count,
							sum(fd.total_sum) as orders_total_sum,
							avg(fd.rate) as rate_avg,
							sum(fd.total_sum)*0.25 as order_processing_fee,
								CASE WHEN avg(fd.rate) < 4 THEN GREATEST(sum(fd.total_sum) * 0.05, 100.0)
				     				 when 4 <= avg(fd.rate) AND avg(fd.rate) < 4.5 THEN 		GREATEST(sum(fd.total_sum) * 0.07, 150.0)
				 					 when 4.5 <= avg(fd.rate) AND avg(fd.rate) < 4.9 THEN GREATEST(sum(fd.total_sum) * 0.08, 175.0)
				     				 when avg(fd.rate) >= 4.9 THEN GREATEST(sum(fd.total_sum) * 0.1, 200.0)
								END "courier_order_sum"	,
							sum(tip_sum) as courier_tips_sum 
 
 
							FROM dds.fct_deliveries fd
					
							LEFT JOIN dds.dm_couriers ddc on ddc.courier_id =fd.courier_id 
							LEFT JOIN dds.dm_orders dmo on dmo.order_key=fd.order_id 
							LEFT JOIN dds.dm_timestamps dt ON dmo.timestamp_id = dt.id

                            GROUP BY   fd.courier_id, ddc.courier_name, settlement_year,settlement_month )
                         	SELECT   temp1.*,
								(courier_order_sum)*0.05 + (courier_tips_sum)* 0.95 as courier_reward_sum
							FROM temp1
   
                   		WHERE   settlement_year  >= %(year)s AND  settlement_month::int >= %(month)s --Пропускаем те объекты, которые уже загрузили.
                    		LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                   
                """, {
                    "year": threshold['year'],
                    "month": threshold['month'],
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs

class CourierLedgerDestRepository:

    def insert_courier_ledger(self, conn: Connection, courier_ledger: CourierLedgerObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                	INSERT INTO cdm.dm_courier_ledger  (   
                		courier_id,
						courier_name,
						settlement_year,
						settlement_month,
						orders_count,
						orders_total_sum,
						rate_avg,
						order_processing_fee,
						courier_order_sum, 
						courier_tips_sum,
						courier_reward_sum
                	
                	)
                  
                    VALUES (    %(courier_id)s  ,   %(courier_name)s , %(settlement_year)s,  %(settlement_month)s, %(orders_count)s,
                    %(orders_total_sum)s,  %(rate_avg)s,   %(order_processing_fee)s, %(courier_order_sum)s, %(courier_tips_sum)s,
                    %(courier_reward_sum)s  )
					ON CONFLICT (courier_id, settlement_year, settlement_month) DO update set 
						--courier_id=excluded.courier_id,
						courier_name=excluded.courier_name,
						--settlement_year=excluded.settlement_year,
						--settlement_month=excluded.settlement_month,
					
						orders_count=excluded.orders_count,
						orders_total_sum=excluded.orders_total_sum,
						rate_avg=excluded.rate_avg,
						order_processing_fee=excluded.order_processing_fee,
						courier_order_sum=excluded.courier_order_sum,
						courier_tips_sum=excluded.courier_tips_sum,
						
						courier_reward_sum=excluded.courier_reward_sum;
                """,
                {
                    
                    
					"courier_id": courier_ledger.courier_id,
					"courier_name": courier_ledger.courier_name,
					"settlement_year": courier_ledger.settlement_year,
					"settlement_month": courier_ledger.settlement_month,
					"orders_count": courier_ledger.orders_count,
					"orders_total_sum": courier_ledger.orders_total_sum,
					"rate_avg": courier_ledger.rate_avg,
		
					"order_processing_fee": courier_ledger.order_processing_fee,
					"courier_order_sum": courier_ledger.courier_order_sum,
					"courier_tips_sum": courier_ledger.courier_tips_sum,
					
					"courier_reward_sum": courier_ledger.courier_reward_sum
                },
            )


class CourierLedgerLoader:
    WF_KEY = "example_courier_ledger_to_cdm_workflow"
    LAST_LOADED_MONTH = "last_loaded_month"
    BATCH_LIMIT = 100000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = CourierLedgerOriginRepository(pg_origin)
        self.cdm = CourierLedgerDestRepository()
        self.settings_repository = CdmEtlSettingsRepository()
        self.log = log

    def dm_load_courier_ledgers(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
             
            	wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_MONTH: {"year": 2022, "month": 1}})

            # Вычитываем очередную пачку объектов.
             
            
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_MONTH]
            print(last_loaded)
            load_queue = self.origin.cdm_courier_ledger(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} courier_ledgers to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for courier_ledger in load_queue:
                self.cdm.insert_courier_ledger(conn, courier_ledger)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_MONTH] = json2str({"year": max([t.settlement_year for t in load_queue]), "month": max([t.settlement_month for t in load_queue])})
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_MONTH]}")
