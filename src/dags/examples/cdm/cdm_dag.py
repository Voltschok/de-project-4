import logging

import pendulum
from airflow.decorators import dag, task
from examples.cdm.dm_settlement_report.dm_settlement_report_loader import DmReportLoader
from examples.cdm.dm_courier_ledger.dm_courier_ledger_loader import CourierLedgerLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_example_cdm_dm_reports_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_reports_load")
    def dm_load_report():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmReportLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.dm_load_reports()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    dm_reports_dict = dm_load_report()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
      # type: ignore
    
    @task(task_id="dm_courier_ledger")
    def dm_load_courier_ledgers():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = CourierLedgerLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.dm_load_courier_ledgers()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    dm_courier_ledger_dict = dm_load_courier_ledgers()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    dm_courier_ledger_dict # type: ignore



cdm_dm_reports_dag = sprint5_example_cdm_dm_reports_dag()
