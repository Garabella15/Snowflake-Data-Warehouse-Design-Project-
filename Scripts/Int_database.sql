/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'ERM_CRM_Database' in snowflake. 
    Additionally, the script sets up three schemas within the database: 'bronze', 'silver', and 'gold' to relect the data 
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/


USE ROLE SYSADMIN;





-- Create the 'DataWarehouse' database
CREATE DATABASE ERP_CRM_DATABASE;
GO

USE ERP_CRM_DATABASE;


-- Create Schemas
CREATE SCHEMA bronze;

CREATE SCHEMA silver;

CREATE SCHEMA gold;




