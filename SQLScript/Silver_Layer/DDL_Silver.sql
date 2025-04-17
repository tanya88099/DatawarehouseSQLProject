/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema. If tables with the same 
    name already exist, they are dropped before being re-created. 

    Use this script to redefine the structure (DDL) of the Silver Layer tables. 
    These tables are designed for standardized and cleansed data derived from 
    the Bronze Layer.
===============================================================================
*/

-- Drop and Create CRM Customer Information Table
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO

CREATE TABLE silver.crm_cust_info (
    cst_id             INT,                -- Customer ID
    cst_key            NVARCHAR(50),      -- Customer Key
    cst_firstname      NVARCHAR(50),      -- First Name of Customer
    cst_lastname       NVARCHAR(50),      -- Last Name of Customer
    cst_marital_status NVARCHAR(50),      -- Marital Status
    cst_gndr           NVARCHAR(50),      -- Gender
    cst_create_date    DATE,              -- Creation Date
    dwh_create_date    DATETIME2 DEFAULT GETDATE() -- Data Warehouse Record Creation Date
);
GO

-- Drop and Create CRM Product Information Table
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info (
    prd_id          INT,                -- Product ID
    cat_id          NVARCHAR(50),      -- Category ID
    prd_key         NVARCHAR(50),      -- Product Key
    prd_nm          NVARCHAR(50),      -- Product Name
    prd_cost        INT,               -- Product Cost
    prd_line        NVARCHAR(50),      -- Product Line
    prd_start_dt    DATE,              -- Product Start Date
    prd_end_dt      DATE,              -- Product End Date
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- Data Warehouse Record Creation Date
);
GO

-- Drop and Create CRM Sales Details Table
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details (
    sls_ord_num     NVARCHAR(50),      -- Sales Order Number
    sls_prd_key     NVARCHAR(50),      -- Sales Product Key
    sls_cust_id     INT,               -- Sales Customer ID
    sls_order_dt    DATE,              -- Sales Order Date
    sls_ship_dt     DATE,              -- Shipment Date
    sls_due_dt      DATE,              -- Due Date
    sls_sales       INT,               -- Total Sales
    sls_quantity    INT,               -- Quantity Sold
    sls_price       INT,               -- Price per Unit
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- Data Warehouse Record Creation Date
);
GO

-- Drop and Create ERP Location Table
IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO

CREATE TABLE silver.erp_loc_a101 (
    cid             NVARCHAR(50),      -- Country ID
    cntry           NVARCHAR(50),      -- Country Name
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- Data Warehouse Record Creation Date
);
GO

-- Drop and Create ERP Customer Details Table
IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO

CREATE TABLE silver.erp_cust_az12 (
    cid             NVARCHAR(50),      -- Customer ID
    bdate           DATE,              -- Birth Date
    gen             NVARCHAR(50),      -- Gender
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- Data Warehouse Record Creation Date
);
GO

-- Drop and Create ERP Product Categories Table
IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
GO

CREATE TABLE silver.erp_px_cat_g1v2 (
    id              NVARCHAR(50),      -- Product ID
    cat             NVARCHAR(50),      -- Category Name
    subcat          NVARCHAR(50),      -- Subcategory Name
    maintenance     NVARCHAR(50),      -- Maintenance Notes
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- Data Warehouse Record Creation Date
);
GO
