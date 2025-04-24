/*
===============================================================================
Database Exploration
===============================================================================
Purpose:
    - To explore the structure of the database, including the list of tables and their schemas.
    - To inspect the columns and metadata for specific tables.

Tables Used:
    - INFORMATION_SCHEMA.TABLES
    - INFORMATION_SCHEMA.COLUMNS
===============================================================================
*/

-- Retrieve a list of all tables in the database to understand available datasets
SELECT 
    TABLE_CATALOG,            -- Name of the database/catalog
    TABLE_SCHEMA,             -- Schema name (e.g., gold, dbo)
    TABLE_NAME,               -- Name of the table
    TABLE_TYPE                -- Type of table (BASE TABLE, VIEW, etc.)
FROM INFORMATION_SCHEMA.TABLES;

-- Retrieve column details for the table 'dim_customers'
SELECT 
    COLUMN_NAME,              -- Name of the column
    DATA_TYPE,                -- Data type (e.g., int, varchar)
    IS_NULLABLE,              -- Indicates whether NULL is allowed
    CHARACTER_MAXIMUM_LENGTH  -- Max character length (for text fields)
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'dim_customers';  -- Restricting to only 'dim_customers' table


/*
===============================================================================
Dimensions Exploration
===============================================================================
Purpose:
    - To explore the structure of dimension tables.

SQL Functions Used:
    - DISTINCT
    - ORDER BY
===============================================================================
*/

-- Get the list of distinct countries where customers are located
SELECT DISTINCT 
    country                 -- Country column from dim_customers
FROM gold.dim_customers
ORDER BY country;           -- Alphabetical order

-- Fetch unique combinations of category, subcategory, and product names
SELECT DISTINCT 
    category,               -- Product category
    subcategory,            -- Product subcategory
    product_name            -- Specific product name
FROM gold.dim_products
ORDER BY category, subcategory, product_name;


/*
===============================================================================
Date Range Exploration 
===============================================================================
Purpose:
    - To determine the temporal boundaries of key data points.
    - To understand the range of historical data.

SQL Functions Used:
    - MIN(), MAX(), DATEDIFF()
===============================================================================
*/

-- Get the earliest and latest order date and the number of months between them
SELECT 
    MIN(order_date) AS first_order_date,                      -- Oldest order date
    MAX(order_date) AS last_order_date,                       -- Most recent order date
    DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS order_range_months
    -- Duration in months between first and last order
FROM gold.fact_sales;

-- Determine the oldest and youngest customer using birthdate
SELECT
    MIN(birthdate) AS oldest_birthdate,                       -- Earliest birthdate
    DATEDIFF(YEAR, MIN(birthdate), GETDATE()) AS oldest_age, -- Approximate age of oldest customer
    MAX(birthdate) AS youngest_birthdate,                     -- Latest birthdate
    DATEDIFF(YEAR, MAX(birthdate), GETDATE()) AS youngest_age-- Approximate age of youngest customer
FROM gold.dim_customers;


/*
===============================================================================
Measures Exploration (Key Metrics)
===============================================================================
Purpose:
    - To calculate aggregated metrics (e.g., totals, averages) for quick insights.
    - To identify overall trends or spot anomalies.

SQL Functions Used:
    - COUNT(), SUM(), AVG()
===============================================================================
*/

-- Calculate total sales revenue
SELECT SUM(sales_amount) AS total_sales FROM gold.fact_sales;

-- Total number of items sold
SELECT SUM(quantity) AS total_quantity FROM gold.fact_sales;

-- Calculate average selling price across all products
SELECT AVG(price) AS avg_price FROM gold.fact_sales;

-- Count of order numbers including duplicates
SELECT COUNT(order_number) AS total_orders FROM gold.fact_sales;

-- Count of distinct order numbers (removes duplicate entries)
SELECT COUNT(DISTINCT order_number) AS total_orders FROM gold.fact_sales;

-- Count total number of product records (may include duplicates)
SELECT COUNT(product_name) AS total_products FROM gold.dim_products;

-- Count of all customer records
SELECT COUNT(customer_key) AS total_customers FROM gold.dim_customers;

-- Count of unique customers who placed at least one order
SELECT COUNT(DISTINCT customer_key) AS total_customers FROM gold.fact_sales;

-- A unified report with all core KPIs using UNION ALL
SELECT 'Total Sales' AS measure_name, SUM(sales_amount) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Quantity', SUM(quantity) FROM gold.fact_sales
UNION ALL
SELECT 'Average Price', AVG(price) FROM gold.fact_sales
UNION ALL
SELECT 'Total Orders', COUNT(DISTINCT order_number) FROM gold.fact_sales
UNION ALL
SELECT 'Total Products', COUNT(DISTINCT product_name) FROM gold.dim_products
UNION ALL
SELECT 'Total Customers', COUNT(customer_key) FROM gold.dim_customers;


/*
===============================================================================
Magnitude Analysis
===============================================================================
Purpose:
    - To quantify data and group results by specific dimensions.
    - For understanding data distribution across categories.

SQL Functions Used:
    - Aggregate Functions: SUM(), COUNT(), AVG()
    - GROUP BY, ORDER BY
===============================================================================
*/

-- Number of customers per country
SELECT
    country,
    COUNT(customer_key) AS total_customers
FROM gold.dim_customers
GROUP BY country
ORDER BY total_customers DESC;

-- Number of customers per gender
SELECT
    gender,
    COUNT(customer_key) AS total_customers
FROM gold.dim_customers
GROUP BY gender
ORDER BY total_customers DESC;

-- Number of products available per category
SELECT
    category,
    COUNT(product_key) AS total_products
FROM gold.dim_products
GROUP BY category
ORDER BY total_products DESC;

-- Average cost of products within each category
SELECT
    category,
    AVG(cost) AS avg_cost
FROM gold.dim_products
GROUP BY category
ORDER BY avg_cost DESC;

-- Revenue generated per product category
SELECT
    p.category,
    SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
GROUP BY p.category
ORDER BY total_revenue DESC;

-- Revenue generated per customer
SELECT
    c.customer_key,
    c.first_name,
    c.last_name,
    SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
GROUP BY 
    c.customer_key,
    c.first_name,
    c.last_name
ORDER BY total_revenue DESC;

-- Total quantity of items sold grouped by customer country
SELECT
    c.country,
    SUM(f.quantity) AS total_sold_items
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
GROUP BY c.country
ORDER BY total_sold_items DESC;


/*
===============================================================================
Ranking Analysis
===============================================================================
Purpose:
    - To rank items (e.g., products, customers) based on performance or other metrics.
    - To identify top performers or laggards.

SQL Functions Used:
    - Window Ranking Functions: RANK(), DENSE_RANK(), ROW_NUMBER(), TOP
    - Clauses: GROUP BY, ORDER BY
===============================================================================
*/

-- Top 5 products by total revenue using simple TOP clause
SELECT TOP 5
    p.product_name,
    SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY total_revenue DESC;

-- Top 5 products by revenue using RANK() for flexible ranking
SELECT *
FROM (
    SELECT
        p.product_name,
        SUM(f.sales_amount) AS total_revenue,
        RANK() OVER (ORDER BY SUM(f.sales_amount) DESC) AS rank_products
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
        ON p.product_key = f.product_key
    GROUP BY p.product_name
) AS ranked_products
WHERE rank_products <= 5;

-- Bottom 5 products in terms of revenue
SELECT TOP 5
    p.product_name,
    SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY total_revenue;

-- Top 10 customers by revenue
SELECT TOP 10
    c.customer_key,
    c.first_name,
    c.last_name,
    SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
GROUP BY 
    c.customer_key,
    c.first_name,
    c.last_name
ORDER BY total_revenue DESC;

-- Bottom 3 customers by number of distinct orders
SELECT TOP 3
    c.customer_key,
    c.first_name,
    c.last_name,
    COUNT(DISTINCT order_number) AS total_orders
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
GROUP BY 
    c.customer_key,
    c.first_name,
    c.last_name
ORDER BY total_orders;
