CREATE TABLE IF NOT EXISTS stg.deliveries (
	id serial4 NOT NULL,
	delivery_id varchar NOT NULL,
	delivery varchar NOT NULL,
	delivery_ts timestamp NOT NULL,
	CONSTRAINT deliveries_delivery_id_key UNIQUE (delivery_id)
);
