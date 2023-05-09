import logging

import pendulum
from airflow.decorators import dag, task
from examples.dds.dm_deliveries.dm_deliveries_loader import DmDeliveryLoader
from examples.dds.dm_couriers.dm_couriers_loader import DmCourierLoader
from examples.dds.fct_deliveries.fct_deliveries_loader import FctDeliveryLoader

from examples.dds.dm_timestamps.dm_timestamps_loader import DmTimeLoader
from examples.dds.dm_users.dm_users_loader import DmUserLoader
from examples.dds.dm_restaurants.dm_restaurants_loader import DmRestaurantLoader
from examples.dds.dm_products.dm_products_loader import DmProductLoader
from examples.dds.dm_orders.dm_orders_loader import DmOrderLoader
from examples.dds.fct_product_sales.fct_product_sales_loader import DmSaleLoader

from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_dds_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_times_load")
    def dm_load_times():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmTimeLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.dm_load_times()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    dm_times_dict = dm_load_times()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    #dm_times_dict  # type: ignore
    
    @task(task_id="dm_users_load")
    def dm_load_users():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmUserLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.dm_load_users()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    dm_users_dict = dm_load_users()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
      # type: ignore
    
    @task(task_id="dm_restaurants_load")
    def dm_load_restaurants():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmRestaurantLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.dm_load_restaurants()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    dm_restaurants_dict = dm_load_restaurants()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
      # type: ignore
    
    @task(task_id="dm_products_load")
    def dm_load_products():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmProductLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.dm_load_products()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    dm_products_dict = dm_load_products()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
      # type: ignore

    @task(task_id="dm_orders_load")
    def dm_load_orders():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmOrderLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.dm_load_orders()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    dm_orders_dict = dm_load_orders()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    
    @task(task_id="dm_sales_load")
    def dm_load_sales():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmSaleLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.dm_load_sales()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    dm_sales_dict = dm_load_sales()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
     # type: ignore
    
    
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_deliveries_load")
    def dm_load_deliveries():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmDeliveryLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.dm_load_deliveries()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    dm_deliveries_dict = dm_load_deliveries()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.

    
    @task(task_id="dm_couriers_load")
    def dm_load_couriers():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmCourierLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.dm_load_couriers()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    dm_couriers_dict = dm_load_couriers()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    dm_couriers_dict >> dm_deliveries_dict    # type: ignore
    
    
    @task(task_id="fct_deliveries_load")
    def fct_load_deliveries():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = FctDeliveryLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.fct_load_deliveries()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    fct_deliveries_dict = fct_load_deliveries()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
      # type: ignore
    [dm_times_dict,  dm_users_dict, dm_restaurants_dict, dm_couriers_dict ] >> dm_products_dict >> dm_orders_dict >> dm_deliveries_dict >> dm_sales_dict >> fct_deliveries_dict  
	     
	
dds_dag = sprint5_dds_dag()
