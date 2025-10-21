create or replace procedure load_silver_layer()
returns varchar not null 
language sql
as
$$
begin 
    --- ===================================================
    ---  Load silver.crm_customer_Info 
    --- step 1. truncate the table 
    --- step 2. performs full load
    --- ===================================================
    truncate table silver.crm_customer_info;
    insert into silver.crm_customer_info
    with dedup_cte as (
    select *
    from bronze.crm_customer_info
    qualify row_number() over(partition by cst_id order by cst_create_date desc) = 1
    )
    select 
    cst_id,
    cst_key,
    trim(cst_firstname) as cst_firstname,
    trim(cst_lastname) as cst_lastname,
    case when upper(cst_martial_status) = 'S' then 'Single'
         when upper(cst_martial_status) = 'M' then 'Married'
         else 'Not available'
    end as cst_martial_status,
    case when upper(cst_gndr) = 'F' then 'Female'
         when upper(cst_gndr) = 'M' then 'Male'
         else 'Not available'
    end as cst_gndr,
    cst_create_date,
    current_timestamp() as dwh_create_date
    from dedup_cte where cst_id is not null;

    --- ===================================================
    ---  Load silver.crm_product_Info 
    --- step 1. truncate the table 
    --- step 2. performs full load
    --- ===================================================
    truncate table silver.crm_product_info;
    insert into silver.crm_product_info
    select 
    prd_id,
    replace(substring(prd_key,1,5),'-','_') as cat_id,
    substring(prd_key,7, len(prd_key)) as prd_key,
    prd_nm,
    ifnull(prd_cost, 0) as prd_cost,
    case upper(trim(prd_line))
        when 'M' then 'Mountain'
        when 'R' then 'Road'
        when 'S' then 'Other sales'
        when 'T' then 'Touring'
        else 'Not Availble'
    end as prd_line,
    pd_start_dt as prd_start_dt,
    lead(pd_start_dt) over (partition by prd_key order by pd_start_dt) as prd_end_dt,
    current_timestamp() as dwh_create_date
    from bronze.crm_product_info;
    
    --- ===================================================
    ---  Load silver.crm_sales_dateils
    --- step 1. truncate the table 
    --- step 2. performs full load
    --- ===================================================
    truncate table silver.crm_sales_details;
    insert into silver.crm_sales_details
    select 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
        else to_date(sls_order_dt::varchar,'YYYYMMDD')
    end as sls_order_dt,
    case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
        else to_date(sls_ship_dt::varchar,'YYYYMMDD')
    end as sls_ship_dt,
    case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
        else to_date(sls_due_dt::varchar,'YYYYMMDD')
    end as sls_due_dt,
    case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
         then sls_quantity * abs(sls_price)
         else sls_sales
    end as sls_sales,
    sls_quantity,
    case when sls_price is null or sls_price <= 0 
         then (sls_sales / nullif(sls_quantity, 0))::numeric
         else sls_price::numeric
    end as sls_price,
    current_timestamp() as dwh_create_date
    from bronze.crm_sales_details;


    --- ===================================================
    ---  Load silver.erp_cust_az12
    --- step 1. truncate the table 
    --- step 2. performs full load
    --- ===================================================
    truncate table silver.erp_cust_az12;
    insert into silver.erp_cust_az12
    select 
    case when cid like 'NAS%' then substring(cid,4,len(cid))
        else cid
    end as cid,
    case when bdate > getdate() then null
        else bdate
    end as bdate,
    case  
         when upper(trim(gen)) in  ('M','Male') then 'Male'
         when upper(trim(gen)) in  ('F', 'Female') then 'Female'
        else 'not available'
    end as gen,
    current_timestamp() as dwh_create_date
    from bronze.erp_cust_az12;

    --- ===================================================
    --- Load silver.erp_loc_a101
    --- step 1. truncate the table 
    --- step 2. performs full load
    --- ===================================================
    truncate table silver.erp_loc_a101;
    insert into silver.erp_loc_a101
    select 
    replace(cid, '-','') as cid,
    case when trim(cntry) = 'DE' then 'Germany'
         when trim(cntry) in ('USA', 'US') then 'United States'
         when cntry is null then 'Not Available'
         else trim(cntry)
    end as cntry,
    current_timestamp() as dwh_create_date
    from bronze.erp_loc_a101;

    --- ===================================================
    --- Load silver.erp_pax_cat_giv2
    --- step 1. truncate the table 
    --- step 2. performs full load
    --- ===================================================
    truncate table silver.erp_px_cat_g1v2;
    insert into silver.erp_px_cat_g1v2
    select
    id,
    cat,
    sub_cat,
    maintenace,
    current_timestamp() as dwh_create_date
    from bronze.erp_px_cat_g1v2;
    
    return 'All bronze-to-silver tables loaded sucessfully';
end
$$
;

call load_silver_layer();
