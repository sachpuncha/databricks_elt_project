-- gender matching
select distinct
        ci.customer_gender,
        cm.gender,
    CASE 
        WHEN ci.customer_gender != "unknown" THEN ci.customer_gender
             ELSE cm.gender
    END AS match_gender
    from datawarehouse.silver.customer_info_cleaned ci
    left join datawarehouse.silver.customers_metadata_cleaned cm
    on ci.customer_key = cm.customer_id
    left join datawarehouse.silver.location_data_cleaned ld
    on ci.customer_key = ld.customer_id

--product_info unique 
select * from datawarehouse.silver.products_info_cleaned
where product_endDate is null

--customer counts

select customer_id, count(*) from
    (select 
        ci.customer_id,
        ci.customer_key,
        ci.customer_firstNname,
        ci.customer_lastName,
        ci.customer_marital_status,
        ci.customer_gender,
        ci.customer_createFullDate,
        cm.birthday,
        cm.gender,
        ld.country,
        ci.customer_createYear,
        ci.customer_createMonth,
        ci.customer_createDay
    from datawarehouse.silver.customer_info_cleaned ci
    left join datawarehouse.silver.customers_metadata_cleaned cm
    on ci.customer_key = cm.customer_id
    left join datawarehouse.silver.location_data_cleaned ld
    on ci.customer_key = ld.customer_id
) group by customer_id
having count (*) > 1


--product category data explore

select ID, count(*) as count from datawarehouse.bronze.product_cat_data_raw
group by ID
having count > 1

select count(*) from datawarehouse.bronze.customers_info_raw;

--customer info data explore

select cst_id, count(*) as count 
from customers_info_raw
group by cst_id
having count > 1;

select * from customers_info_raw where cst_gndr not in ("F","M");

select * from customers_info_raw where cst_marital_status not in ("S","M");

select cst_id from customers_info_raw where (cst_lastname is null) or (cst_firstname is null);

select cst_id from customers_info_raw where cst_create_date is null;

-- product information data explroe

select * from datawarehouse.bronze.products_info_raw
where prd_id is null;

select * from datawarehouse.bronze.products_info_raw
where prd_key is null;

select distinct(prd_line) from datawarehouse.bronze.products_info_raw
