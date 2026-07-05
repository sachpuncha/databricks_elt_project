create or replace table datawarehouse.gold.dim_products as
select 
    row_number() over (order by pi.product_id) as uid,
    pi.product_id,
    pi.product_key,
    pi.category_id,
    pi.product_name,
    pc.category,
    pc.sub_category,
    pi.product_cost,
    pi.product_line,
    pi.product_startDate,
    pc.maintenance
from datawarehouse.silver.products_info_cleaned as pi
left join datawarehouse.silver.product_category_cleaned pc
on pi.category_id = pc.id
order by pi.product_startDate

