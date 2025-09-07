USE CATALOG hive_metastore;
USE SCHEMA delta_db;

show tables;

select * from hive_metastore.delta_db.orders_delta;
select count(*) from hive_metastore.delta_db.orders_delta;


select * from  hive_metastore.delta_db._wm_orders;