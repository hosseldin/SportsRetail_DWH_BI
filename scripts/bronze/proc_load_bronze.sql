/*==============================================================
 Author		: Hossam Mahmoud
 Purpose	: Stored Procedure: Load Bronze Layer (Source -> Bronze)
 Date			: 2025-08-11
 Notes		: 
 Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
     It performs the following actions:
		- Truncates the bronze tables before loading data.
		- Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

 Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

 Usage Example:
    EXEC bronze.load_bronze;
==============================================================*/

-- Switches to this Database
USE SR_DWH_DB;
GO

EXEC bronze.load_bronze

-- Create the procedure (or alter if it already exists)
CREATE OR ALTER PROCEDURE bronze.load_bronze
	-- We'll declare variables for the paths so we don't hardcode it everytime which is redundant and time consuming
	@crm_path NVARCHAR(500) = 'G:\SSIS_ETL_PROJECTS\SportsRetail_DWH_BI\datasets\source_crm\', 
	@erp_path NVARCHAR(500) = 'G:\SSIS_ETL_PROJECTS\SportsRetail_DWH_BI\datasets\source_erp\'
AS
BEGIN

	SET NOCOUNT ON;	-- Why this
    
	-- Declare variables to track timing
    DECLARE @start_time DATETIME, @end_time DATETIME, 
            @batch_start_time DATETIME, @batch_end_time DATETIME, 
            @sql NVARCHAR(MAX); 

    BEGIN TRY  -- Start error-handling block

        -- Record when the whole batch starts
        SET @batch_start_time = GETDATE();

        -- Log header
        PRINT '================================================';
        PRINT 'Start Loading Bronze Layer';
        PRINT '================================================';

        ------------------------------------------------
        -- Load CRM (Customer Relationship Management) Tables
        ------------------------------------------------
        PRINT '------------------------------------------------';
        PRINT '>> Loading CRM Tables';
        PRINT '------------------------------------------------';

        -- 1) Load crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info; -- Clear old data

        PRINT '>> Inserting Data Into: bronze.crm_cust_info';
        SET @sql = N'
        BULK INSERT bronze.crm_cust_info
        FROM ''' + @crm_path + 'cust_info.csv' + '''
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = '','',
            TABLOCK
        );';
        EXEC sp_executesql @sql;  

		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' ms';
        PRINT '>> -------------';


        -- 2) Load crm_prd_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		SET @sql = N'
        BULK INSERT bronze.crm_prd_info
        FROM ''' + @crm_path + 'prd_info.csv' + ''' 
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = '','',
            ROWTERMINATOR = ''0x0a'',
            TABLOCK
        );';
        EXEC sp_executesql @sql;  
        
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' ms';
        PRINT '>> -------------';


        -- 3) Load crm_sales_details
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        SET @sql = N'
        BULK INSERT bronze.crm_sales_details
        FROM ''' + @crm_path + 'sales_details.csv' + '''
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = '','',
            ROWTERMINATOR = ''0x0a'',
            TABLOCK
        );';
        EXEC sp_executesql @sql;  

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' ms';
        PRINT '>> -------------';


        ------------------------------------------------
        -- Load ERP (Enterprise Resource Planning) Tables
        ------------------------------------------------
        PRINT '------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '------------------------------------------------';

        -- 4) Load erp_loc_a101
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
        SET @sql = N'
        BULK INSERT bronze.erp_loc_a101
        FROM ''' + @erp_path + 'loc_a101.csv' + '''
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = '','',
            ROWTERMINATOR = ''0x0a'',
            TABLOCK
        );';
        EXEC sp_executesql @sql;  

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' ms';
        PRINT '>> -------------';


        -- 5) Load erp_cust_az12
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
        SET @sql = N'
        BULK INSERT bronze.erp_cust_az12
        FROM ''' + @erp_path + 'cust_az12.csv' + '''
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = '','',
            ROWTERMINATOR = ''0x0a'',
            TABLOCK
        );';
        EXEC sp_executesql @sql;  

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' ms';
        PRINT '>> -------------';


        -- 6) Load erp_px_cat_g1v2
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
        SET @sql = N'
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM ''' + @erp_path + 'px_cat_g1v2.csv' + ''' 
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = '','',
            ROWTERMINATOR = ''0x0a'',
            TABLOCK
        );';
        EXEC sp_executesql @sql;  

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' ms';
        PRINT '>> -------------';

        -- Record when the whole batch ends
        SET @batch_end_time = GETDATE();

        -- Log success summary
        PRINT '=========================================='
        PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(millisecond, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' ms';
        PRINT '=========================================='

    END TRY
    BEGIN CATCH  -- If any error occurs

        -- Print error details
        PRINT '=========================================='
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
        PRINT 'Error Message ' + ERROR_MESSAGE();
        PRINT 'Error Number ' + CAST (ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State  ' + CAST (ERROR_STATE() AS NVARCHAR);
        PRINT '=========================================='

    END CATCH
END
