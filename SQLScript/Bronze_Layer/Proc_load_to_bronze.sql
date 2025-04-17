-- Stored Procedure: Load Data into Bronze Layer
-- Batch Start Time - Batch End Time = Total Time to Insert Data into Bronze Layer
-- Start Time - End Time = Total Time to Bulk Insert the Particular CSV Data into the Database
-- Existing data in the table is truncated before a bulk insert using the SQL command:
-- BULK INSERT db.tablename FROM 'path' WITH (OPTIONS)

USE master;
USE DataWarehouse;

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT 'Loading Bronze Layer';
        PRINT '*****************************************';

        -- Load CRM Customer Information Table
        PRINT 'Processing: bronze.crm_cust_info';
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_cust_info;
        PRINT 'Data truncated for bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\Tanya\Documents\Project\DWH_PROJECT\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration (bronze.crm_cust_info): ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '================================================';

        -- Load CRM Product Information Table
        PRINT 'Processing: bronze.crm_prd_info';
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_prd_info;
        PRINT 'Data truncated for bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\Tanya\Documents\Project\DWH_PROJECT\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration (bronze.crm_prd_info): ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '================================================';

        -- Load CRM Sales Details Table
        PRINT 'Processing: bronze.crm_sales_details';
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_sales_details;
        PRINT 'Data truncated for bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\Tanya\Documents\Project\DWH_PROJECT\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration (bronze.crm_sales_details): ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '================================================';

        -- Load ERP Location Data Table
        PRINT 'Processing: bronze.erp_loc_a101';
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_loc_a101;
        PRINT 'Data truncated for bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\Tanya\Documents\Project\DWH_PROJECT\loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration (bronze.erp_loc_a101): ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '================================================';

        -- Load ERP Customer Data Table
        PRINT 'Processing: bronze.erp_cust_az12';
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_cust_az12;
        PRINT 'Data truncated for bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\Tanya\Documents\Project\DWH_PROJECT\cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration (bronze.erp_cust_az12): ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '================================================';

        -- Load ERP Product Category Table
        PRINT 'Processing: bronze.erp_px_cat_g1v2';
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        PRINT 'Data truncated for bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\Tanya\Documents\Project\DWH_PROJECT\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration (bronze.erp_px_cat_g1v2): ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '================================================';

        -- Total Batch Load Duration
        SET @batch_end_time = GETDATE();
        PRINT 'Bronze Layer Loading Completed';
        PRINT '>> Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '*****************************************';
    END TRY
    BEGIN CATCH
        PRINT '++++++++++++++++++++++++++++';
        PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT 'Line: ' + CAST(ERROR_LINE() AS NVARCHAR);
        PRINT 'Procedure: ' + ISNULL(ERROR_PROCEDURE(), 'N/A');
        PRINT '++++++++++++++++++++++++++++';
    END CATCH
END;

-- Execute the procedure
EXEC bronze.load_bronze;

-- Verify the data
SELECT * FROM bronze.erp_loc_a101;
