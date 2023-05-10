from datetime import datetime, date, timedelta
from logging import Logger

from examples.stg import EtlSetting, StgEtlSettingsRepository
from examples.stg.deliveries_dag.pg_saver_deliveries import PgSaverDeliveries
from examples.stg.deliveries_dag.delivery_reader import  DeliveryReader
from lib import PgConnect
from lib.dict_util import json2str



class DeliveryLoader:
     
	 
    _LOG_THRESHOLD = 2
    
    _SESSION_LIMIT = 50

    WF_KEY = "example_deliveries_origin_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_id"

    def __init__(self, collection_loader: DeliveryReader , pg_dest: PgConnect, pg_saver: PgSaverDeliveries, logger: Logger) -> None:
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

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:

                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        # JSON ничего не знает про даты. Поэтому записываем строку, которую будем кастить при использовании.
                        # А в БД мы сохраним именно JSON.
                        self.LAST_LOADED_TS_KEY: (date.today()-timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
                    }
                )

 		
            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_id = datetime.fromisoformat(last_loaded_ts_str)          
            
            load_queue = self.collection_loader.get_deliveries( self._SESSION_LIMIT, last_loaded_id, 0)

            #self.log.info(f"Found {len(load_queue)} documents to sync from deliveries collection.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            i = 0
            
            for d in load_queue:

                self.pg_saver.save_object(conn, str(d['delivery_id']) , d["delivery_ts"], d)
                
                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"processed {i} documents of {len(list(load_queue))} while syncing deliveries.")

             
            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t["delivery_ts"] for t in load_queue]) 
            
            print(wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return 0#len(load_queue)
