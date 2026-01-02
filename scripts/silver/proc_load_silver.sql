/*
= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
Stored Procedure: Load Silver Layer (Bronze -> Silver)
= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to
    populate the 'silver' schema tables from the 'bronze' schema.

Actions Performed:
    - Truncates Silver tables.
    - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;

= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =   */


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '=======================================';
        PRINT 'Loading Silver Layer';
        PRINT '=======================================';

        PRINT '---------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '---------------------------------------';



        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;
        print '>> Inserting Data Into: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info(
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )SELECT 
        cst_id,
        cst_key,
        TRIM(cst_firstname) as cst_first_name, /* data transformation by trimming spaces */
        TRIM(cst_lastname) as cst_last_name,
        CASE WHEN UPPER (TRIM (cst_marital_status)) = 'M' THEN 'Married'  /* data normalization & standardiziation - converting to meaningful values */
            WHEN UPPER (TRIM (cst_marital_status)) = 'S' THEN 'Single'
            ELSE 'n/a'
        END cst_marital_status,
        CASE WHEN UPPER (TRIM(cst_gndr)) = 'F' THEN 'Female' 
            WHEN UPPER (TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'     /* Handling missing values */
        END cst_gndr,
        cst_create_date
        FROM (
            select *,
            ROW_NUMBER() OVER (PARTITION BY cst_id order BY cst_create_date desc) as flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL    /* removing duplicates */
        )t WHERE flag_last = 1
        set @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST (DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds' ;
        PRINT '>> -----------------------------------' ; 




        set @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST (DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds' ;
        PRINT '>> -----------------------------------' ; 
        PRINT '>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;
        print '>> Inserting Data Into: silver.crm_prd_info'
        INSERT INTO silver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT 
            prd_id,
            REPLACE (SUBSTRING(prd_key, 1, 5), '-' , '_') AS cat_id,
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
            prd_nm,
            ISNULL(prd_cost, 0) AS prd_cost,
            CASE UPPER(TRIM(prd_line)) 
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ElSE 'n/a'
            END AS prd_line,
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            CAST(     /*prd_end_dt*/
                LEAD(prd_start_dt) OVER (PARTITION by prd_key ORDER BY prd_start_dt)-1  --data enrichment
                AS DATE
            ) AS prd_end_dt
        FROM bronze.crm_prd_info ;
        set @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST (DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds' ;
        PRINT '>> -----------------------------------' ;






        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;
        print '>> Inserting Data Into: silver.crm_sales_details'
        INSERT into silver.crm_sales_details(
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
        SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
            else cast(cast(sls_order_dt as varchar) as date)
        end as sls_order_dt,
        case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
            else CAST(CAST(sls_ship_dt as varchar)as date) 
        end as sls_ship_dt,
        case when sls_due_dt = 0 or len(sls_due_dt) !=8 then null
            else CAST(CAST(sls_due_dt as varchar)as date)
        end as sls_due_dt,
        case when sls_sales <= 0 or sls_sales is NULL or sls_sales != sls_quantity * ABS(sls_price) 
            then sls_quantity * ABS(sls_price)
            else sls_sales
        end as sls_sales,   -- Recalculate sales if original values are missing or incorrect
        sls_quantity,
        case when sls_price <= 0 or sls_price is null 
            then sls_sales/ NULLIF(sls_quantity, 0)
            else sls_price  -- derrive price if original values are missingor incorrect
        end as sls_price
        FROM bronze.crm_sales_details;
        set @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST (DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds' ;
        PRINT '>> -----------------------------------' ;





        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;
        print '>> Inserting Data Into: silver.erp_cust_az12'
        INSERT INTO silver.erp_cust_az12(cid,bdate,gen)
        SELECT 
        case when cid like 'NAS%' then SUBSTRING(cid, 4, LEN(cid))
            else cid
        end as cid,
        case when bdate > GETDATE() then null
            else bdate
        end bdate,
        CASE 
                WHEN UPPER(
                    REPLACE(
                    REPLACE(
                    REPLACE(
                    LTRIM(RTRIM(gen)),
                    CHAR(13), ''),
                    CHAR(10), ''),
                    CHAR(160), '')
                ) IN ('F', 'FEMALE') THEN 'Female'

                WHEN UPPER(
                    REPLACE(
                    REPLACE(
                    REPLACE(
                    LTRIM(RTRIM(gen)),
                    CHAR(13), ''),
                    CHAR(10), ''),
                    CHAR(160), '')
                ) IN ('M', 'MALE') THEN 'Male'

                ELSE 'n/a'
            END AS gen
        FROM bronze.erp_cust_az12;
        set @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST (DATEDIFF(SECOND ,@start_time,@end_time) AS NVARCHAR) + ' seconds' ;
        PRINT '>> -----------------------------------' ;



        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;
        print '>> Inserting Data Into: silver.erp_loc_a101'
        INSERT INTO silver.erp_loc_a101(cid, cntry)
        SELECT
            REPLACE(cid, '-', '') AS cid,
            CASE 
                WHEN UPPER(
                    REPLACE(
                    REPLACE(
                    REPLACE(LTRIM(RTRIM(cntry)),
                            CHAR(13), ''),
                            CHAR(10), ''),
                            CHAR(160), '')
                ) = 'DE' THEN 'Germany'

                WHEN UPPER(
                    REPLACE(
                    REPLACE(
                    REPLACE(LTRIM(RTRIM(cntry)),
                            CHAR(13), ''),
                            CHAR(10), ''),
                            CHAR(160), '')
                ) IN ('US', 'USA') THEN 'United States'

                WHEN cntry IS NULL
                    OR REPLACE(
                        REPLACE(
                        REPLACE(LTRIM(RTRIM(cntry)),
                                CHAR(13), ''),
                                CHAR(10), ''),
                                CHAR(160), '') = ''
                THEN 'n/a'

                ELSE
                    REPLACE(
                    REPLACE(
                    REPLACE(LTRIM(RTRIM(cntry)),
                            CHAR(13), ''),
                            CHAR(10), ''),
                            CHAR(160), '')
            END AS cntry
        FROM bronze.erp_loc_a101;
        set @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST (DATEDIFF(SECOND ,@start_time,@end_time) AS NVARCHAR) + ' seconds' ;
        PRINT '>> -----------------------------------' ;



        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        print '>> Inserting Data Into: silver.erp_px_cat_g1v2'
        INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        select 
        id,
        cat,
        subcat,
        LTRIM(RTRIM(REPLACE(REPLACE(maintenance, CHAR(13), ''), CHAR(10), ''))) as maintenance
        from bronze.erp_px_cat_g1v2;
        set @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST (DATEDIFF(SECOND ,@start_time,@end_time) AS NVARCHAR) + ' seconds' ;
        PRINT '>> -----------------------------------' ;



        SET @batch_end_time = GETDATE();   
        PRINT '=================================================';
        PRINT 'Loading Bronze Layer is Completed';
        PRINT 'Total Load Duration: ' + CAST (DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds' ;
        PRINT '=================================================';


        END TRY
            BEGIN CATCH
                PRINT '=================================================';
                PRINT 'ERROR OCCUED DURING LOADING BRONZE LAYER';
                PRINT 'Error Message' + ERROR_MESSAGE();
                PRINT 'Error Number' + CAST (ERROR_NUMBER() AS NVARCHAR);
                PRINT 'Error State' + CAST (ERROR_STATE() AS NVARCHAR);
                PRINT '=================================================';
        END CATCH
END
