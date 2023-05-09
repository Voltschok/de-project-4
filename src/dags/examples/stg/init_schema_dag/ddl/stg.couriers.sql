CREATE TABLE IF NOT EXISTS  stg.couriers (
	id serial4 NOT NULL,
	courier_id varchar NOT NULL,
	courier varchar NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT couriers_courier_id_key UNIQUE (courier_id)
);
