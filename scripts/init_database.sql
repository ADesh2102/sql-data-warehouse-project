
**NOTE:**
This script will:
- Drop the existing `DataWarehouse` database if it exists.
- Recreate a fresh `DataWarehouse` database.
- Create the following schemas inside it: `bronze`, `silver`, and `gold`.

**WARNING:**
Running this script will permanently delete all existing data, tables, and objects inside `DataWarehouse`.  
Use this only for development or testing purposes.  
**Do not use in production** unless you're absolutely sure!
\c postgres

DROP DATABASE IF EXISTS "DataWarehouse";
CREATE DATABASE "DataWarehouse";

\c "DataWarehouse"

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
