/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates **views** for the **Gold layer** in the data warehouse.

    The Gold layer contains **cleaned, conformed, and enriched data** in a 
    **star schema format**, which is used directly by BI tools and dashboards 
    for reporting and analytics.

    The Gold layer includes:
        - Dimension tables: dim_customers, dim_products
        - Fact table: fact_sales

    These views aggregate and join data from the Silver layer and apply:
        - Business rules
        - Surrogate key generation
        - Filtering out irrelevant or historical records
        - Column renaming for readability and standardization

Usage:
    These views should be created after the Silver layer is populated and 
    quality checks are completed. They are read-only analytical layers.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- Description: Contains enriched customer data including demographics
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,        -- Surrogate primary key for customers
    ci.cst_id                          AS customer_id,           -- Unique customer ID from CRM
    ci.cst_key                         AS customer_number,       -- Alternate customer key
    ci.cst_firstname                   AS first_name,            -- Customer first name
    ci.cst_lastname                    AS last_name,             -- Customer last name
    la.cntry                           AS country,               -- Country information from ERP location
    ci.cst_marital_status              AS marital_status,        -- Marital status from CRM
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr               -- Prefer CRM gender if present
        ELSE COALESCE(ca.gen, 'n/a')                             -- Fallback to ERP gender if CRM says 'n/a'
    END                                AS gender,                
    ca.bdate                           AS birthdate,             -- Customer birthdate from ERP
    ci.cst_create_date                 AS create_date            -- When the customer was created
FROM silver.crm_cust_info ci                                   -- CRM customer info
LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid       -- Join with ERP customer info on customer key
LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key = la.cid;       -- Join with ERP location info on customer key
GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- Description: Contains active product details and category hierarchy
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,  -- Surrogate primary key for products
    pn.prd_id       AS product_id,       -- Unique product ID
    pn.prd_key      AS product_number,   -- Alternate product key
    pn.prd_nm       AS product_name,     -- Product name
    pn.cat_id       AS category_id,      -- Category ID used for mapping
    pc.cat          AS category,         -- Category name from ERP
    pc.subcat       AS subcategory,      -- Subcategory from ERP
    pc.maintenance  AS maintenance,      -- Maintenance tag/category
    pn.prd_cost     AS cost,             -- Product cost
    pn.prd_line     AS product_line,     -- Product line information
    pn.prd_start_dt AS start_date        -- Start date of the product’s availability
FROM silver.crm_prd_info pn                                     -- CRM product info
LEFT JOIN silver.erp_px_cat_g1v2 pc ON pn.cat_id = pc.id        -- Join with ERP category table
WHERE pn.prd_end_dt IS NULL;                                    -- Filter out expired/inactive products
GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- Description: Contains transactional sales data at product and customer level
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num  AS order_number,     -- Sales order number
    pr.product_key  AS product_key,      -- Foreign key to gold.dim_products
    cu.customer_key AS customer_key,     -- Foreign key to gold.dim_customers
    sd.sls_order_dt AS order_date,       -- Order placed date
    sd.sls_ship_dt  AS shipping_date,    -- Order shipped date
    sd.sls_due_dt   AS due_date,         -- Order due date
    sd.sls_sales    AS sales_amount,     -- Total sales amount
    sd.sls_quantity AS quantity,         -- Quantity sold
    sd.sls_price    AS price             -- Price per unit
FROM silver.crm_sales_details sd                             -- Source sales data from CRM
LEFT JOIN gold.dim_products pr ON sd.sls_prd_key = pr.product_number  -- Join to product dimension
LEFT JOIN gold.dim_customers cu ON sd.sls_cust_id = cu.customer_id;   -- Join to customer dimension
GO

-- =============================================================================
-- Sample Queries (for testing purposes)
-- =============================================================================
SELECT * FROM gold.dim_customers;
SELECT * FROM gold.dim_products;
SELECT * FROM gold.fact_sales;
