CREATE TABLE IF NOT EXISTS  dds.dm_timestamps (
	id serial4 NOT NULL,
	ts timestamp NOT NULL,
	"year" int2 NOT NULL,
	"month" int2 NOT NULL,
	"day" int2 NOT NULL,
	"time" time NOT NULL,
	"date" date NOT NULL,
	CONSTRAINT dm_timestamps_day_check CHECK (((day >= 1) AND (day <= 31))),
	CONSTRAINT dm_timestamps_month_check CHECK (((month >= 1) AND (month <= 12))),
	CONSTRAINT dm_timestamps_pkey PRIMARY KEY (id),
	CONSTRAINT dm_timestamps_year_check CHECK (((year >= 2022) AND (year < 2500)))
);

CREATE TABLE IF NOT EXISTS  dds.dm_restaurants (
	id serial4 NOT NULL,
	restaurant_id varchar NOT NULL,
	restaurant_name varchar NOT NULL,
	active_from timestamp NOT NULL,
	active_to timestamp NOT NULL,
	CONSTRAINT dm_restaurants_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS  dds.dm_users (
	id serial4 NOT NULL,
	user_id varchar NOT NULL,
	user_name varchar NOT NULL,
	user_login varchar NOT NULL,
	CONSTRAINT dm_users_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS dds.dm_products (
	id serial4 NOT NULL,
	restaurant_id int4 NOT NULL,
	product_id varchar NOT NULL,
	product_name varchar NOT NULL,
	product_price numeric(14, 2) NOT NULL DEFAULT 0,
	active_from timestamp NOT NULL,
	active_to timestamp NOT NULL,
	CONSTRAINT dm_products_pkey PRIMARY KEY (id),
	CONSTRAINT dm_products_product_id_key UNIQUE (product_id),
	CONSTRAINT dm_products_product_price_check CHECK ((product_price >= (0)::numeric))
);

CREATE TABLE IF NOT EXISTS dds.dm_orders (
	id serial4 NOT NULL,
	user_id int4 NOT NULL,
	restaurant_id int4 NOT NULL,
	timestamp_id int4 NOT NULL,
	order_key varchar NOT NULL,
	order_status varchar NOT NULL,
	CONSTRAINT dm_orders_order_key_key UNIQUE (order_key),
	CONSTRAINT dm_orders_pkey PRIMARY KEY (id)
);
CREATE TABLE IF NOT EXISTS  dds.dm_couriers (
	id int4 NOT NULL,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	CONSTRAINT dm_couriers_courier_id_key UNIQUE (courier_id),
	CONSTRAINT dm_couriers_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS  dds.dm_deliveries (
	id int4 NOT NULL,
	order_id varchar NOT NULL,
	order_ts timestamp NOT NULL,
	delivery_id varchar NOT NULL,
	courier_id varchar NOT NULL,
	address varchar NOT NULL,
	delivery_ts timestamp NOT NULL,
	rate int4 NOT NULL,
	sum numeric(14, 2) NOT NULL,
	tip_sum numeric(14, 2) NOT NULL,
	CONSTRAINT dm_deliveries_check CHECK (((rate >= 1) AND (rate <= 5))),
	CONSTRAINT dm_deliveries_order_id_key UNIQUE (order_id),
	CONSTRAINT dm_deliveries_pkey PRIMARY KEY (id)
);


-- dds.dm_deliveries foreign keys
ALTER TABLE dds.dm_deliveries DROP CONSTRAINT IF EXISTS dm_deliveries_courier_id_fkey;  
ALTER TABLE dds.dm_deliveries DROP CONSTRAINT IF EXISTS dm_deliveries_order_id_fkey;  

ALTER TABLE dds.dm_deliveries ADD CONSTRAINT dm_deliveries_courier_id_fkey FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(courier_id);
ALTER TABLE dds.dm_deliveries ADD CONSTRAINT dm_deliveries_order_id_fkey FOREIGN KEY (order_id) REFERENCES dds.dm_orders(order_key);

CREATE TABLE IF NOT EXISTS  dds.fct_product_sales (
	id serial4 NOT NULL,
	product_id int4 NOT NULL,
	order_id int4 NOT NULL,
	count int4 NOT NULL DEFAULT 0,
	price numeric(14, 2) NOT NULL DEFAULT 0,
	total_sum numeric(14, 2) NOT NULL DEFAULT 0,
	bonus_payment numeric(14, 2) NOT NULL DEFAULT 0,
	bonus_grant numeric(14, 2) NOT NULL DEFAULT 0,
	CONSTRAINT fct_product_sales_bonus_grant_check CHECK ((bonus_grant >= (0)::numeric)),
	CONSTRAINT fct_product_sales_bonus_payment_check CHECK ((bonus_payment >= (0)::numeric)),
	CONSTRAINT fct_product_sales_count_check CHECK ((count >= 0)),
	CONSTRAINT fct_product_sales_pkey PRIMARY KEY (id),
	CONSTRAINT fct_product_sales_price_check CHECK ((price >= (0)::numeric)),
	CONSTRAINT fct_product_sales_product_id_order_id_key UNIQUE (product_id, order_id),
	CONSTRAINT fct_product_sales_total_sum_check CHECK ((total_sum >= (0)::numeric))
);


-- dds.fct_product_sales foreign keys
ALTER TABLE dds.fct_product_sales DROP CONSTRAINT IF EXISTS fct_product_sales_order_id_fkey;
ALTER TABLE dds.fct_product_sales ADD CONSTRAINT fct_product_sales_order_id_fkey FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id);


CREATE TABLE IF NOT EXISTS  dds.fct_deliveries (
	id serial4 NOT NULL,
	courier_id varchar NOT NULL,
	delivery_id varchar NOT NULL,
	delivery_ts timestamp NOT NULL,
	order_id varchar NOT NULL,
	rate int4 NOT NULL,
	total_sum numeric(14, 2) NOT NULL DEFAULT 0,
	tip_sum numeric(14, 2) NOT NULL DEFAULT 0,
	CONSTRAINT fct_deliveries_order_id_key UNIQUE (order_id),
	CONSTRAINT fct_deliveries_pkey PRIMARY KEY (id),
	CONSTRAINT fct_deliveries_tip_sum_check CHECK ((tip_sum >= (0)::numeric)),
	CONSTRAINT fct_deliveries_total_sum_check CHECK ((total_sum >= (0)::numeric))
);

