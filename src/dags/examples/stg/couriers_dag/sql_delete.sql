delete from stg.ordersystem_orders cascade;
delete from stg.ordersystem_users cascade;
delete from stg.ordersystem_restaurants cascade;
delete from stg.bonussystem_events cascade;
delete from stg.bonussystem_ranks  cascade;
delete from stg.bonussystem_users  cascade;
delete from stg.srv_wf_settings cascade;
delete from dds.srv_wf_settings cascade;
delete from stg.couriers cascade;

delete from cdm.dm_settlement_report  cascade;

delete from dds.dm_restaurants  cascade;
delete from dds.dm_cs  cascade;
delete from dds.dm_users  cascade;
delete from dds.dm_timestamps  cascade;
delete from dds.dm_orders  cascade;
delete from dds.fct_product_sales  cascade;
alter table couriers add column update_ts timestamp;

select id,  object_value::json->>'_id' as restaurant_id,
			object_value::json->>'name' as restaurant_name,
			(object_value::json->>'update_ts')::timestamp as active_from,
			'2099-12-31'::timestamp as active_to
		    FROM stg.ordersystem_restaurants
		    
		    
select id,  courier::json->>'name' as courier_name 
		    FROM stg.couriers c 
		    
 
	
DROP TABLE stg.couriers;

CREATE TABLE stg.couriers (
	id serial4 NOT NULL,
	courier_id varchar not null unique,
	courier varchar NOT null,
	update_ts timestamp not NULL
);


DROP TABLE stg.deliveries ;

CREATE TABLE stg.deliveries (
	id serial4 NOT NULL,
	delivery_id varchar not null unique,
	delivery varchar NOT null,
	update_ts timestamp not NULL
);
