from logging import Logger
from typing import List
from datetime import datetime, date, time
from examples.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DmTimeObj(BaseModel):
    id: int
    ts: datetime
    year: int
    month: int
    day: int
    date: date 
    time:  time
   

class DmTimesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def dm_list_times(self, time_threshold: int, limit: int) -> List[DmTimeObj]:
        with self._db.client().cursor(row_factory=class_row(DmTimeObj)) as cur:
            cur.execute(
                """
                
                SELECT id, (object_value::json->>'date')::timestamp as ts,
                    EXTRACT(YEAR from (object_value::json->>'date')::timestamp) as year,
                    EXTRACT(MONTH from (object_value::json->>'date')::timestamp) as month,
                    EXTRACT(DAY from (object_value::json->>'date')::timestamp) as day,
                    (object_value::json->>'date'):: timestamp ::date as date,
                    (object_value::json->>'date'):: timestamp ::time as time
			 
			
		        FROM stg.ordersystem_orders

                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": time_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
            print(objs)
        return objs


class DmTimeDestRepository:

    def insert_time(self, conn: Connection, time: DmTimeObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                	INSERT INTO dds.dm_timestamps (id, ts,year,month, day, date, time)
                  
                    VALUES (%(id)s, %(ts)s,  %(year)s  ,   %(month)s, %(day)s, %(date)s , %(time)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                         
                        ts=EXCLUDED.ts,
                        year=EXCLUDED.year,
                        month=EXCLUDED.month,
                        day=EXCLUDED.day,
                        
                        date=EXCLUDED.date,
                        time=EXCLUDED.time;
                """,
                {
                        "id": time.id,
                        "ts": time.ts,
                        "year": time.year,
                        "month": time.month,
                        "day": time.day,
                        "date": time.date,
                        "time": time.time
    			
    			
                },
            )


class DmTimeLoader:
    WF_KEY = "example_times_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmTimesOriginRepository(pg_origin)
        self.dds = DmTimeDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def dm_load_times(self):
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
            load_queue = self.origin.dm_list_times(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} dm_times to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for dm_time in load_queue:
                self.dds.insert_time(conn, dm_time)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
