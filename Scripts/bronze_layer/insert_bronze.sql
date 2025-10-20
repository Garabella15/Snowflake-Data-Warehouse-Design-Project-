/*








*/









---- manual insert into the the bronzer layer erp 
insert into bronze.erp_cust_az12
select 
    $1 AS CID, 
    $2 AS BDATE, 
    $3 AS GEN
from @erp_sources/CUST_AZ12.csv
(file_format => ingestion_file);


insert into bronze.erp_loc_a101
select 
  $1 CID, 
  $2 AS CNTRY
from @erp_sources/LOC_A101.csv
(file_format => ingestion_file);

insert into bronze.erp_px_cat_g1v2
select 
    $1 as ID, 
    $2 AS CAT, 
    $3 AS SUBCAT, 
    $4 AS MAINTENANCE 
from @erp_sources/PX_CAT_G1V2.csv
(file_format => ingestion_file);


-- manual insert in the the bronxer layer from crm source system 

insert into bronze.crm_customer_info
select 
    $1 AS cst_id, 
    $2 AS cst_key, 
    $3 AS cst_firstname,
    $4 as cst_lastname,
    $5 as cst_martial_status,
    $6 as cst_gndr,
    $7 as cst_create_date 
from @crm_source/cust_info.csv
(file_format => ingestion_file);


insert into bronze.crm_product_info
select 
    $1 as prd_id, 
    $2 AS prd_key,
    $3 as prd_nm,
    $4 as prd_cost,
    $5 as prd_line,
    $6 as prd_start_dt,
    $7 as prd_end_dt
from @crm_source/prd_info.csv
(file_format => ingestion_file);

insert into bronze.crm_sales_details
select 
    $1 as sls_ord_num, 
    $2 AS sls_prd_key, 
    $3 AS sls_cust_id, 
    $4 AS sls_order_dt,
    $5 as sls_ship_dt,
    $6 as sls_due_dt,
    $7 as sls_sales,
    $8 as sls_quantity,
    $9 as sls_price
from @crm_source/sales_details.csv
(file_format => ingestion_file);
