SET SEARCH_PATH TO gold;

-- Views were created for the gold layer and load the data from the silver layer.

---------------------------- join the customer details related tables ---------------------------- 
CREATE VIEW gold.dim_customers AS
SELECT
    row_number() OVER (ORDER BY cst_id) AS Customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS Country,
    ci.cst_marital_status AS marial_status,
    CASE
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr  -- CRM is the Main data source as per the business rule.
        ELSE coalesce(ci.cst_gndr, ca.gen)
    END AS gender,
    ca.bdate AS birth_date,
    ci.cst_create_date AS create_date

FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON ca.cid = ci.cst_key
LEFT JOIN silver.erp_loc_a101 AS la
ON la.cid = ci.cst_key;





----------------------------  join the product details related tables ---------------------------- 
CREATE OR REPLACE VIEW gold.dim_product AS
SELECT
    row_number() OVER (ORDER BY prd_start_dt, prd_key) AS product_key,
    pi.prd_id AS product_id,
    pi.prd_key AS product_number,
    pi.prd_nm AS product_name,
    pi.cat_id AS category_id,
    pc.cat AS categry,
    pc.subcat AS sub_category,
    pc.maintenance,
    pi.prd_cost AS cost,
    pi.prd_line AS product_line,
    pi.prd_start_dt AS start_date
FROM silver.crm_prd_info AS pi
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
ON pi.cat_id = pc.id
WHERE pi.prd_end_dt IS NULL;


----------------------------  join the sales and dim tables ---------------------------- 
CREATE OR REPLACE VIEW gold.fact_sales AS
SELECT sls_ord_num AS order_nbumber,
       pr.product_key,
       dc.customer_id,
       sls_order_dt AS order_date,
       sls_ship_dt AS shipping_date,
       sls_due_dt AS due_date,
       sls_sales AS sales_amount,
       sls_quantity AS quantity,
       sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_product pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers dc
ON sd.sls_cust_id = dc.customer_id;
