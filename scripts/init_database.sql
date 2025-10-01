/**********************************************************************************************
    Script Purpose: 
        This script will DROP and RECREATE the "DataWarehouse" database in PostgreSQL.
        After recreating, it sets up the following schemas within the database:
            - bronze
            - silver
            - gold
    
    WARNING:
        ⚠️ Running this script will PERMANENTLY DELETE the existing "DataWarehouse" database 
        along with all objects and data inside it. 
        Ensure you have proper backups before execution. 
**********************************************************************************************/

-- Drop and recreate the DataWarehouse database
DROP DATABASE IF EXISTS datawarehouse;
CREATE DATABASE datawarehouse;


-- STEP 2: Run this after connecting to "datawarehouse"

-- Create Schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
