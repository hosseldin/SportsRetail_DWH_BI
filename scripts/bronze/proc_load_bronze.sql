/*==============================================================
 Author     : Hossam Mahmoud
 Purpose    : Stored Procedure: Load Bronze Layer (Source -> Bronze)
 Date       : 2025-08-11
 Notes      : 
 Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
        - Truncates the bronze tables before loading data.
        - Uses OPENROWSET BULK to load data from CSV files into bronze tables.

 Parameters:
    None. 
    This stored procedure does not accept any parameters or return any values.

 Usage Example:
    EXEC bronze.load_bronze;
==============================================================*/

CREATE OR ALTER PROCEDURE bronze.load_bronze
    @crm_path NVARCHAR(500) = 'G:\SSIS_ETL_PROJECTS\SportsRetail_DWH_BI\datasets\source_crm\', 
    @erp_path NVARCHAR(500) = 'G:\SSIS_ETL_PROJECTS\SportsRetail_DWH_BI\datasets\source_erp\'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start_time DATETIME, @end_time DATETIME,
            @batch_start_time DATETIME, @batch_end_time DATETIME,
            @sql NVARCHAR(MAX);

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '================================================';
        PRINT 'Start Loading Bronze Layer';
        PRINT '================================================';

        ------------------------------------------------
        -- CRM TABLES
        ------------------------------------------------
        PRINT '------------------------------------------------';
        PRINT '>> Loading CRM Tables';
        PRINT '------------------------------------------------';

        -- crm_cust_info
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_cust_info;

        SET @sql = N'
            INSERT INTO bronze.crm_cust_info
            SELECT *
            FROM OPENROWSET(
                BULK ''' + @crm_path + 'cust_info.csv' + ''',
                FORMAT = ''CSV'',
                FIRSTROW = 2
            ) AS data;';
        EXEC sp_executesql @sql;

        SET @end_time = GETDATE();
        PRINT '>> crm_cust_info loaded in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' sec';

        -- crm_prd_info
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_prd_info;

        SET @sql = N'
            INSERT INTO bronze.crm_prd_info
            SELECT *
            FROM OPENROWSET(
                BULK ''' + @crm_path + 'prd_info.csv' + ''',
                FORMAT = ''CSV'',
                FIRSTROW = 2
            ) AS data;';
        EXEC sp_executesql @sql;

        SET @end_time = GETDATE();
        PRINT '>> crm_prd_info loaded in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' sec';

        -- crm_sales_details
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_sales_details;

        SET @sql = N'
            INSERT INTO bronze.crm_sales_details
            SELECT *
            FROM OPENROWSET(
                BULK ''' + @crm_path + 'sales_details.csv' + ''',
                FORMAT = ''CSV'',
                FIRSTROW = 2
            ) AS data;';
        EXEC sp_executesql @sql;

        SET @end_time = GETDATE();
        PRINT '>> crm_sales_details loaded in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' sec';


        ------------------------------------------------
        -- ERP TABLES
        ------------------------------------------------
        PRINT '------------------------------------------------';
        PRINT '>> Loading ERP Tables';
        PRINT '------------------------------------------------';

        -- erp_loc_a101
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_loc_a101;

        SET @sql = N'
            INSERT INTO bronze.erp_loc_a101
            SELECT *
            FROM OPENROWSET(
                BULK ''' + @erp_path + 'loc_a101.csv' + ''',
                FORMAT = ''CSV'',
                FIRSTROW = 2
            ) AS data;';
        EXEC sp_executesql @sql;

        SET @end_time = GETDATE();
        PRINT '>> erp_loc_a101 loaded in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' sec';

        -- erp_cust_az12
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_cust_az12;

        SET @sql = N'
            INSERT INTO bronze.erp_cust_az12
            SELECT *
            FROM OPENROWSET(
                BULK ''' + @erp_path + 'cust_az12.csv' + ''',
                FORMAT = ''CSV'',
                FIRSTROW = 2
            ) AS data;';
        EXEC sp_executesql @sql;

        SET @end_time = GETDATE();
        PRINT '>> erp_cust_az12 loaded in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' sec';

        -- erp_px_cat_g1v2
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        SET @sql = N'
            INSERT INTO bronze.erp_px_cat_g1v2
            SELECT *
            FROM OPENROWSET(
                BULK ''' + @erp_path + 'px_cat_g1v2.csv' + ''',
                FORMAT = ''CSV'',
                FIRSTROW = 2
            ) AS data;';
        EXEC sp_executesql @sql;

        SET @end_time = GETDATE();
        PRINT '>> erp_px_cat_g1v2 loaded in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' sec';


        ------------------
