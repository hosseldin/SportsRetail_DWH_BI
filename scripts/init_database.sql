
/*==============================================================
 Author		: Hossam Mahmoud
 Purpose	: Create Database and Schemas
 Date			: 2025-08-11
 Notes		: 
Script Purpose:
	This script creates a new database named 'SR_DWH_DB' after checking if it already exists. 
	If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
	within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
	Running this script will drop the entire 'SR_DWH_DB' database if it exists. 
	All data in the database will be permanently deleted. Proceed with caution 
	and ensure you have proper backups before running this script.
==============================================================*/

-- Switch context to the 'master' database
USE master;
GO

-- Drop and recreate the 'SR_DWH_DB' database
ALTER DATABASE SR_DWH_DB SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
DROP DATABASE IF EXISTS SR_DWH_DB;
GO

-- Create the 'SR_DWH_DB' database
CREATE DATABASE SR_DWH_DB;
GO

USE SR_DWH_DB;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
