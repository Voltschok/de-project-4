import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from examples.stg.order_system_users_dag.pg_saver import PgSaverUsers
from examples.stg.order_system_users_dag.user_loader import UserLoader
from examples.stg.order_system_users_dag.user_reader import UserReader

from examples.stg.order_system_orders_dag.pg_saver import PgSaverOrders
from examples.stg.order_system_orders_dag.order_loader import OrderLoader
from examples.stg.order_system_orders_dag.order_reader import OrderReader

from examples.stg.order_system_restaurants_dag.pg_saver import PgSaverRestaurants
from examples.stg.order_system_restaurants_dag.restaurant_loader import RestaurantLoader
from examples.stg.order_system_restaurants_dag.restaurant_reader import RestaurantReader

from examples.stg.couriers_dag.pg_saver_couriers import PgSaverCouriers
from examples.stg.couriers_dag.courier_loader import CourierLoader
from examples.stg.couriers_dag.courier_reader import  CourierReader

from examples.stg.deliveries_dag.pg_saver_deliveries import PgSaverDeliveries
from examples.stg.deliveries_dag.delivery_loader import DeliveryLoader
from examples.stg.deliveries_dag.delivery_reader import  DeliveryReader


 
from lib import ConnectionBuilder, MongoConnect


log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'example', 'stg', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_stg_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Получаем переменные из Airflow.
    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task()
    def load_users():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver_users = PgSaverUsers()

        # Инициализируем подключение у MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = UserReader(mongo_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader =UserLoader(collection_reader, dwh_pg_connect, pg_saver_users, log)

        # Запускаем копирование данных.
        loader.run_copy()

    user_loader = load_users()

    # Задаем порядок выполнения. Таск только один, поэтому зависимостей нет.
    user_loader  # type: ignore
    
    @task()
    def load_restaurants():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver_restaurants = PgSaverRestaurants()

        # Инициализируем подключение у MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = RestaurantReader(mongo_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = RestaurantLoader(collection_reader, dwh_pg_connect, pg_saver_restaurants, log)

        # Запускаем копирование данных.
        loader.run_copy()

    restaurant_loader = load_restaurants()

    # Задаем порядок выполнения. Таск только один, поэтому зависимостей нет.
    restaurant_loader  # type: ignore
    @task()
    def load_orders():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver_orders = PgSaverOrders()

        # Инициализируем подключение у MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = OrderReader(mongo_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = OrderLoader(collection_reader, dwh_pg_connect, pg_saver_orders, log)

        # Запускаем копирование данных.
        loader.run_copy()

    order_loader = load_orders()
    
    @task(task_id="load_couriers")
    def load_couriers():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver_couriers = PgSaverCouriers()
        collection_reader=CourierReader()

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        
        loader =CourierLoader(collection_reader, dwh_pg_connect, pg_saver_couriers, log)

        # Запускаем копирование данных.
        loader.run_copy()

    courier_loader = load_couriers()
    
    @task(task_id="load_deliveries")
    def load_deliveries():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver_deliveries = PgSaverDeliveries()

 
	# Инициализируем класс, реализующий чтение данных из источника.
        collection_reader3=DeliveryReader()
	
	# Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader =DeliveryLoader(collection_reader3, dwh_pg_connect, pg_saver_deliveries, log)

        # Запускаем копирование данных
        loader.run_copy()

    delivery_loader = load_deliveries()

stg_order_system_dag = sprint5_stg_dag()  # noqa
