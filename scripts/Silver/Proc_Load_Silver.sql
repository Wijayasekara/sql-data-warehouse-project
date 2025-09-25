SET SEARCH_PATH TO silver;

CREATE OR REPLACE PROCEDURE silver.Load_silver()
LANGUAGE plpgsql
AS
$$
    DECLARE
        stat_time time;
        end_time time;
        Proc_exe_time interval;
    BEGIN
        stat_time = now();
-- ......................................................................................................................
        BEGIN
             -- Load Customer Data
            RAISE NOTICE 'Loading the customer data!';
            TRUNCATE TABLE silver.crm_cust_info;
            INSERT INTO silver.crm_cust_info
                (
                cst_id,
                cst_key,
                cst_firstname,
                cst_lastname,
                cst_marital_status,
                cst_gndr,
                cst_create_date
                )

            SELECT
                cst_id,
                cst_key,
                trim(cst_firstname),
                trim(cst_lastname),
                  CASE
                    WHEN trim(upper(cst_marital_status)) = 'M' THEN 'Married'
                    WHEN trim(upper(cst_marital_status)) = 'S' THEN 'Single'
                    ELSE 'N/A'
                END cst_marital_status,
                  CASE
                    WHEN trim(upper(cst_gndr)) = 'M' THEN 'Male'
                    WHEN trim(upper(cst_gndr)) = 'F' THEN 'Female'
                    ELSE 'N/A'
                END cst_gndr,
                cst_create_date
            FROM
                (SELECT
                    cst_id,
                    cst_key,
                    cst_firstname,
                    cst_lastname,
                    cst_marital_status,
                    cst_gndr,
                    cst_create_date,
                    row_number() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC ) AS flag_last
                    FROM bronze.crm_cust_info
                ) as temp WHERE flag_last = 1;
            EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Error loading customer data : %', SQLERRM;
        END;


-- ......................................................................................................................

        BEGIN
               -- Load prd data
            RAISE NOTICE 'Loading the prd data!';
            TRUNCATE TABLE silver.crm_prd_info;
            INSERT INTO silver.crm_prd_info
            (
                   prd_id,
                   cat_id,
                   prd_key,
                   prd_nm,
                   prd_cost,
                   prd_line,
                   prd_start_dt,
                   prd_end_dt
            )
            SELECT prd_id,
                   replace(substring(prd_key,1,5),'-', '_')AS Cat_id,
                   substring(prd_key, 7, length(prd_key)) AS Prod_key,
                   prd_nm,
                   coalesce(prd_cost, 0) AS prd_cost,
                  CASE
                      WHEN trim(prd_line) = 'M' THEN 'Mountain'
                      WHEN trim(prd_line) = 'R' THEN 'Road'
                      WHEN trim(prd_line) = 'S' THEN 'Other Sales'
                      WHEN trim(prd_line) = 'T' THEN 'Touring'
                      ELSE 'n/a'
                  END AS Prd_line,
                   prd_start_dt,
                   lead(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 NewEndDate
            FROM bronze.crm_prd_info;
            EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Error loading prd data : %', SQLERRM;
        END;

-- ......................................................................................................................
        BEGIN
            -- Load sales data
            RAISE NOTICE 'Loading the sales data!';
            TRUNCATE TABLE silver.crm_sales_details;
            INSERT INTO silver.crm_sales_details
                (
                   sls_ord_num,
                   sls_prd_key,
                   sls_cust_id,
                   sls_order_dt,
                   sls_ship_dt,
                   sls_due_dt,
                   sls_sales,
                   sls_quantity,
                   sls_price
                )

            SELECT sls_ord_num,
                   sls_prd_key,
                   sls_cust_id,

            --        Fixing the date columns
                   CASE
                       WHEN sls_order_dt = 0 OR length(sls_order_dt::text) != 8 THEN NULL
                       ELSE to_date(sls_order_dt :: text,'YYYYMMDD')
                    END AS sls_order_dt,
                CASE
                       WHEN sls_ship_dt = 0 OR length(sls_ship_dt::text) != 8 THEN NULL
                       ELSE (sls_ship_dt ::text) :: date
                    END AS sls_ship_dt,
                 CASE
                       WHEN sls_due_dt = 0 OR length(sls_due_dt::text) != 8 THEN NULL
                       ELSE (sls_due_dt ::text) :: date
                    END AS sls_due_dt,
            --     Fixing the sales  column
                CASE
                    WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity *sls_price THEN sls_quantity * abs(sls_price)
                    ELSE sls_sales
                END AS sls_sales,

                --     fixing the QTY column
               CASE
                    WHEN sls_quantity IS NULL OR sls_quantity <= 0 THEN sls_sales / nullif(sls_price,0)
                    ELSE sls_quantity
                END AS sls_quantity,

            --     fixing the sales price column
                  CASE
                    WHEN sls_price IS NULL OR sls_price <= 0 THEN  sls_sales / nullif(sls_quantity,0)
                    ELSE sls_price
                END AS sls_price

            FROM bronze.crm_sales_details;
            EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Error loading sales data : %', SQLERRM;

        END;

-- ......................................................................................................................
        BEGIN
             -- Load az12 data
            RAISE NOTICE 'Loading the az12 data!';
            TRUNCATE TABLE silver.erp_cust_az12;
            INSERT INTO silver.erp_cust_az12(cid, bdate, gen)
        SELECT
               CASE
                   WHEN cid LIKE 'NAS%' THEN substring(cid, 4, length(cid))
                   ELSE cid
               END AS new_ID,
            CASE
                WHEN bdate > now() THEN NULL
                ELSE bdate
            END AS new_bdate,
            CASE
                WHEN upper(trim(gen)) IN ('F','FEMALE') THEN 'Female'
                WHEN upper(trim(gen)) IN ('M','MALE') THEN 'Male'
                ELSE 'n/a'
            END New_gen
        FROM bronze.erp_cust_az12;
            EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Error loading az12 data : %', SQLERRM;

        END;

-- ......................................................................................................................
        BEGIN
            -- Load a101 data
            RAISE NOTICE 'Loading the a101 data!';
            TRUNCATE TABLE silver.erp_loc_a101;
            INSERT INTO silver.erp_loc_a101(cid, cntry)
            SELECT
                replace(cid,'-','') AS new_cid,
                CASE
                    WHEN trim(cntry) IN ('DE','GERMANY') THEN 'Germany'
                    WHEN trim(cntry) IN ('US','UNITED STATES') THEN 'United States'
                    WHEN trim(cntry) = '' OR cntry IS NULL THEN 'n/a'
                ELSE trim(cntry) END as new_cntry
            FROM bronze.erp_loc_a101;
            EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Error loading a101 data : %', SQLERRM;

        END;

-- ......................................................................................................................

        BEGIN
            -- load g1v2 data
            RAISE NOTICE 'Loading the g1v2 data!';
            TRUNCATE TABLE silver.erp_px_cat_g1v2;
            INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
            SELECT id, cat, subcat, maintenance
            FROM bronze.erp_px_cat_g1v2;
            EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Error loading g1v1 data : %', SQLERRM;

        END;
        end_time = now();

--      Proc execute time calculation:
        Proc_exe_time = end_time -stat_time;
        RAISE NOTICE 'Proc_exected within: %', Proc_exe_time;
    END;
$$;

CALL silver.Load_silver();

