-- Select the Bonze Schema to load te data from the csv files.
SET SEARCH_PATH TO bronze;

/*
1. PostgresSQL’s COPY FROM runs on the database server, not from the client (pgAdmin or Pycharm/Data Grip).
2. That means the file must be in a location where the Postgres server process (running on your Mac) can read it.
3. The /tmp directory is world-readable and accessible by Postgres.
4. So the CSV file where placed there to ensure the server has permission to load it.
*/

CREATE OR REPLACE PROCEDURE bronze.Load_bronze()
LANGUAGE plpgsql
AS
$$
DECLARE
    start_time timestamp;
    end_time timestamp;
    proc_start timestamp;
    proc_end timestamp;
    proc_exe_time interval;
BEGIN

    proc_start := now();

    -- CRM Full Load
    RAISE NOTICE '.........Loading the CRM dataset to bronze layer!.........';
    start_time := now();
    RAISE NOTICE 'Start time: %', start_time;

    BEGIN
        TRUNCATE TABLE bronze.crm_cust_info;
        COPY bronze.crm_cust_info
        FROM '/private/tmp/Datawarehouse_Files/source_crm/cust_info.csv'
        DELIMITER ','
        CSV HEADER;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading crm_cust_info: %', SQLERRM;
    END;

    BEGIN
        TRUNCATE TABLE bronze.crm_prd_info;
        COPY bronze.crm_prd_info
        FROM '/private/tmp/Datawarehouse_Files/source_crm/prd_info.csv'
        DELIMITER ','
        CSV HEADER;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading crm_prd_info: %', SQLERRM;
    END;

    BEGIN
        TRUNCATE TABLE bronze.crm_sales_details;
        COPY bronze.crm_sales_details
        FROM '/private/tmp/Datawarehouse_Files/source_crm/sales_details.csv'
        DELIMITER ','
        CSV HEADER;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading crm_sales_details: %', SQLERRM;

    END;

    end_time := now();
    RAISE NOTICE 'End time: %', end_time;

    -- ERP Full Load
    RAISE NOTICE '.........Loading the ERP dataset to bronze layer!.........';
    start_time := now();
    RAISE NOTICE 'Start time: %', start_time;

    BEGIN
        TRUNCATE TABLE bronze.erp_cust_az12;
        COPY bronze.erp_cust_az12
        FROM '/private/tmp/Datawarehouse_Files/source_erp/CUST_AZ12.csv'
        DELIMITER ','
        CSV HEADER;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading erp_cust_az12: %', SQLERRM;
    END;

    BEGIN
        TRUNCATE TABLE bronze.erp_loc_a101;
        COPY bronze.erp_loc_a101
        FROM '/private/tmp/Datawarehouse_Files/source_erp/LOC_A101.csv'
        DELIMITER ','
        CSV HEADER;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading erp_loc_a101: %', SQLERRM;
    END;

    BEGIN
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        COPY bronze.erp_px_cat_g1v2
        FROM '/private/tmp/Datawarehouse_Files/source_erp/PX_CAT_G1V2.csv'
        DELIMITER ','
        CSV HEADER;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading erp_px_cat_g1v2: %', SQLERRM;
    END;

    end_time := now();
    RAISE NOTICE 'End time: %', end_time;

    proc_end := now();
    proc_exe_time := proc_end - proc_start;
    RAISE NOTICE '>>>>>>>Proc Exe time recorded as: %<<<<<<<<', proc_exe_time;

END;
$$;

-- Call the proc when needed to load the data.

CALL bronze.Load_bronze()

