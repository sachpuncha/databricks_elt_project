create or replace table datawarehouse.gold.fact_sales as 
select 
    sd.order_number,
    c.uid as customer_uid,
    p.uid as products_uid,
    sd.quantity,
    sd.unit_price,
    sd.total_sales,
    sd.order_date,
    sd.ship_date,
    sd.due_date
from datawarehouse.silver.sales_details_cleaned sd
left join datawarehouse.gold.dim_customers  c
on sd.customer_id = c.customer_id
left join datawarehouse.gold.dim_products p
on sd.product_key = p.product_key
order by sd.order_date
