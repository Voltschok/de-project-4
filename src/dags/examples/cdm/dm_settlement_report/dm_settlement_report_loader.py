from logging import Logger
from typing import List
from datetime import datetime, date
from examples.cdm.cdm_settings_repository import EtlSetting, CdmEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DmReportObj(BaseModel):
	 
	restaurant_id: str
	restaurant_name: str
	settlement_date: date 
	orders_count: int
	orders_total_sum: float
	orders_bonus_payment_sum: float
	orders_bonus_granted_sum: float
	order_processing_fee: float
	restaurant_reward_sum: float
	last_order_dt: datetime

class DmReportsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def dm_list_reports(self, report_threshold: str, limit: int) -> List[DmReportObj]:
        with self._db.client().cursor(row_factory=class_row(DmReportObj)) as cur:
            cur.execute(
                """
					WITH test11 as( 
						select distinct
						 
						dmo.restaurant_id, 
						dmr.restaurant_name, 
						dmt.date as settlement_date,
						count(distinct dmo.id) as orders_count, 
						sum(fct.total_sum) as orders_total_sum, 
						sum(fct.bonus_payment) as orders_bonus_payment_sum, 
						sum(fct.bonus_grant) as orders_bonus_granted_sum,
						sum(0.25*fct.total_sum) as order_processing_fee, 
						sum(fct.total_sum  - 0.25*fct.total_sum - fct.bonus_payment) as  restaurant_reward_sum,
						MAX(dmt.ts) AS last_order_dt
						
					FROM dds.dm_restaurants dmr

						LEFT JOIN dds.dm_products dp on dp.restaurant_id =dmr.id 
						LEFT JOIN   dds.fct_product_sales fct on fct.product_id=dp.id
						LEFT JOIN  dds.dm_orders dmo on dmo.id=fct.order_id 
						LEFT JOIN   dds.dm_timestamps dmt on dmt.id=dmo.timestamp_id
						GROUP BY dmo.restaurant_id, dmr.restaurant_name, dmt.date, dmo.order_status 
						HAVING dmo.order_status='CLOSED') 
					SELECT 
 						restaurant_id,
						restaurant_name,
						settlement_date,
						orders_count,
						orders_total_sum,
						orders_bonus_payment_sum,
						orders_bonus_granted_sum,
						order_processing_fee,
						restaurant_reward_sum,
						last_order_dt
                       
                	            	
					FROM test11 
                    WHERE test11.last_order_dt > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                   -- ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                   
                """, {
                    "threshold": report_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
            print(objs)
        return objs


class DmReportDestRepository:

    def insert_report(self, conn: Connection, report: DmReportObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                	INSERT INTO cdm.dm_settlement_report  ( 
                	restaurant_id,
					restaurant_name,
					settlement_date,
					orders_count,
					orders_total_sum,
					orders_bonus_payment_sum,
					orders_bonus_granted_sum,
					order_processing_fee, 
					restaurant_reward_sum
                	
                	)
                  
                    VALUES (   %(restaurant_id)s  ,   %(restaurant_name)s , %(settlement_date)s,  %(orders_count)s,
                    %(orders_total_sum)s,  %(orders_bonus_payment_sum)s, %(orders_bonus_granted_sum)s, %(order_processing_fee)s,
                    %(restaurant_reward_sum)s  )
			ON CONFLICT on CONSTRAINT chec DO update set 
					restaurant_id=excluded.restaurant_id,
					restaurant_name=excluded.restaurant_name,
					settlement_date=excluded.settlement_date,
					orders_count=excluded.orders_count,
					orders_total_sum=excluded.orders_total_sum,
					orders_bonus_payment_sum=excluded.orders_bonus_payment_sum,
					orders_bonus_granted_sum=excluded.orders_bonus_granted_sum,
					order_processing_fee=excluded.order_processing_fee,
					restaurant_reward_sum=excluded.restaurant_reward_sum;
                """,
                {
                    
                     
					"restaurant_id": report.restaurant_id,
					"restaurant_name": report.restaurant_name,
					"settlement_date": report.settlement_date,
					"orders_count": report.orders_count,
					"orders_total_sum": report.orders_total_sum,
					"orders_bonus_payment_sum": report.orders_bonus_payment_sum,
					"orders_bonus_granted_sum": report.orders_bonus_granted_sum,
					"order_processing_fee": report.order_processing_fee,
					"restaurant_reward_sum": report.restaurant_reward_sum
                },
            )


class DmReportLoader:
    WF_KEY = "example_reports_origin_to_cdm_workflow"
    LAST_LOADED_DATE_KEY = "last_loaded_data"
    BATCH_LIMIT = 100000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmReportsOriginRepository(pg_origin)
        self.cdm = DmReportDestRepository()
        self.settings_repository = CdmEtlSettingsRepository()
        self.log = log

    def dm_load_reports(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
             
            	wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_DATE_KEY : '2020-01-01 00:01:00'})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_DATE_KEY]
            load_queue = self.origin.dm_list_reports(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} dm_reports to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for dm_report in load_queue:
                self.cdm.insert_report(conn, dm_report)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_DATE_KEY] = max([t.last_order_dt for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_DATE_KEY]}")
