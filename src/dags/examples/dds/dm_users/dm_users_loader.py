from logging import Logger
from typing import List

from examples.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DmUserObj(BaseModel):
    id: int
    user_id: str
    user_name: str
    user_login: str
   

class DmUsersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def dm_list_users(self, user_threshold: int, limit: int) -> List[DmUserObj]:
        with self._db.client().cursor(row_factory=class_row(DmUserObj)) as cur:
            cur.execute(
                """
                    SELECT  id,  object_value::json->>'_id' as user_id, object_value::json->>'name' as user_name, object_value::json->>'login' as user_login
 		    FROM stg.ordersystem_users
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": user_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
            print(objs)
        return objs


class DmUserDestRepository:

    def insert_user(self, conn: Connection, user: DmUserObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                	INSERT INTO dds.dm_users (id, user_id,user_name,user_login)
                  
                    VALUES (%(id)s, %(user_id)s,  %(user_name)s  ,   %(user_login)s )
                    ON CONFLICT (id) DO UPDATE
                    SET
                         
                        user_id=EXCLUDED.user_id,
                        user_name=EXCLUDED.user_name,
                        user_login=EXCLUDED.user_login;
                """,
                {
                        "id": user.id,
                        "user_id": user.user_id,
                        "user_name": user.user_name,
                        "user_login": user.user_login
                },
            )


class DmUserLoader:
    WF_KEY = "example_users_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmUsersOriginRepository(pg_origin)
        self.dds = DmUserDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def dm_load_users(self):
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
            load_queue = self.origin.dm_list_users(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} dm_users to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for dm_user in load_queue:
                self.dds.insert_user(conn, dm_user)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
