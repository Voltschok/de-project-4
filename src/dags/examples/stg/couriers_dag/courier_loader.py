from datetime import datetime
from logging import Logger

from examples.stg import EtlSetting, StgEtlSettingsRepository
from examples.stg.couriers_dag.pg_saver_couriers import PgSaverCouriers
from examples.stg.couriers_dag.courier_reader import  CourierReader
from lib import PgConnect
from lib.dict_util import json2str



class CourierLoader:
     

    _LOG_THRESHOLD = 2
    _SESSION_LIMIT = 50
    

    WF_KEY = "example_couriersystem_couriers_origin_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_id"

    def __init__(self, collection_loader: CourierReader, pg_dest: PgConnect, pg_saver: PgSaverCouriers, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger

    def run_copy(self) -> int:
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        self.LAST_LOADED_TS_KEY: ""
                    }
                )

 
            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            self.log.info(f"starting to load from last checkpoint: {last_loaded_id}")
            load_queue = self.collection_loader.get_couriers(self._SESSION_LIMIT, 0)
           
            
            self.log.info(f"Found {len(load_queue)} documents to sync from couriers collection.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            i = 0
            for d in load_queue:  
                self.pg_saver.save_object(conn, str(d["_id"]), d)
                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"processed {i} documents of {len(list(load_queue))} while syncing couriers.")
            
            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t["_id"] for t in load_queue]) 
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return len(load_queue)
