
  create view `etl_demo`.`stg_orders__dbt_tmp`
    
    
  as (
    select order_id,customer_id,order_date,amount from etl_demo.src_orders
  );