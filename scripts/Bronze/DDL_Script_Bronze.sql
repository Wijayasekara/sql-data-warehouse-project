/*

This following script was used to create the tables in the bronze layer.
Table names and the column names as similar to the csv files. No change to the structure made in the bronze layer as per the data architecture. 

*/

-- Select the Bonze Schema for the table creation.
SET SEARCH_PATH TO bronze;

--  CRM table creation for the Bronze Layer. As-is structure.

DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info
(
    cst_id             int,
    cst_key            text,
    cst_firstname      text,
    cst_lastname       text,
    cst_marital_status text,
    cst_gndr           text,
    cst_create_date    date
);

DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info
(
    prd_id int,
    prd_key text,
    prd_nm text,
    prd_cost int,
    prd_line text,
    prd_start_dt date,
    prd_end_dt date
);

DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details
(

    sls_ord_num text,
    sls_prd_key text,
    sls_cust_id int,
    sls_order_dt int,
    sls_ship_dt int,
    sls_due_dt int,
    sls_sales int,
    sls_quantity int,
    sls_price int

);

--  ERP table creation for the Bronze Layer. As-is structure.

DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12
(
    CID   text,
    BDATE date,
    GEN   text
);

DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101
(
    CID text,
    CNTRY text

);

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2
(
    ID text,
    CAT text,
    SUBCAT text,
    MAINTENANCE text

);
