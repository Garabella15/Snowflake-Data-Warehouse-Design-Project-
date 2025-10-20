/*

================================================================================
DDL SCripts: Create Bronze Tables

================================================================================

Objective:
    To create tables in the bronze schema

=================================================================================
*/


/*--- create data objects in the bronze layer for crm source */

create or replace table crm_customer_info (
cst_id number,
cst_key varchar(50),
cst_firstname varchar(50),
cst_lastname varchar(50),
cst_martial_status varchar(50),
cst_gndr varchar(50),
cst_create_date date
);


create or replace table crm_product_info (
prd_id number,
prd_key varchar(50),
prd_nm varchar(50),
prd_cost number,
prd_line varchar(2),
pd_start_dt date,
prd_end_dt date
);

create or replace table crm_sales_details (
sls_ord_num number,
sls_odr_key varchar(50),
sls_cust_id number,
sls_order_dt date,
sls_ship_dt date,
sls_due_dt date,
sls_sales number,
sls_quantity number,
sls_price number
);


/* --- create data object in bronze layer for erp source system */ 

create or replace table erp_cust_az12 (
cid varchar(50),
bdate date,
gen varchar(20)
);


create or replace table erp_loc_a101 (
cid varchar(50),
cntry varchar(50)
);


create or replace table erp_px_cat_g1v2 (
id varchar(50),
cat varchar(50),
sub_cat varchar(50),
maintenace varchar(50)
);
