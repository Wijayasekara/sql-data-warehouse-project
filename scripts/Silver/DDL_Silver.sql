-- Select the Bonze Schema for the table creation.
SET SEARCH_PATH TO silver;

-- The script used to create table in the bronze layer was taken to create tables in the silver layers as well.
-- Additionally, a new column called created date added as a part of data engineering.
-- A new naming convention was used for the new column added during data engineering to identify it easily.

DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info
(
    cst_id             int,
    cst_key            text,
    cst_firstname      text,
    cst_lastname       text,
    cst_marital_status text,
    cst_gndr           text,
    cst_create_date    date,
    dwh_creation_date timestamp DEFAULT now()  -- Metadata added during data engineering.
);

DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info
(
    prd_id int,
    cat_id text,
    prd_key text,
    prd_nm text,
    prd_cost int,
    prd_line text,
    prd_start_dt date,
    prd_end_dt date,
    dwh_creation_date timestamp DEFAULT now() -- Metadata added during data engineering.
);

DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details
(

    sls_ord_num text,
    sls_prd_key text,
    sls_cust_id int,
    sls_order_dt date,
    sls_ship_dt date,
    sls_due_dt date,
    sls_sales int,
    sls_quantity int,
    sls_price int,
    dwh_creation_date timestamp DEFAULT now() -- Metadata added during data engineering.
);

--  ERP table creation for the silver Layer. As-is structure.

DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12
(
    CID   text,
    BDATE date,
    GEN   text,
    dwh_creation_date timestamp DEFAULT now() -- Metadata added during data engineering.
);

DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101
(
    CID text,
    CNTRY text,
    dwh_creation_date timestamp DEFAULT now() -- Metadata added during data engineering.
);

DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2
(
    ID text,
    CAT text,
    SUBCAT text,
    MAINTENANCE text,
    dwh_creation_date timestamp DEFAULT now() -- Metadata added during data engineering.
);
