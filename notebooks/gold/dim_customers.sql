
create or replace table datawarehouse.gold.dim_customers as
select 
        row_number() over (order by ci.customer_id) as uid,
        ci.customer_id,
        ci.customer_key,
        ci.customer_firstNname,
        ci.customer_lastName,
        CASE 
            WHEN ci.customer_gender != "unknown" THEN ci.customer_gender
             ELSE cm.gender
        END AS customer_gender,
        ci.customer_marital_status,
        cm.birthday,
        ld.country,
        ci.customer_createYear,
        ci.customer_createMonth,
        ci.customer_createDay
    from datawarehouse.silver.customer_info_cleaned ci
    left join datawarehouse.silver.customers_metadata_cleaned cm
    on ci.customer_key = cm.customer_id
    left join datawarehouse.silver.location_data_cleaned ld
    on ci.customer_key = ld.customer_id
