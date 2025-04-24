/*
===============================================================================
Change Over Time Analysis
===============================================================================
Purpose:
    - To track trends, growth, and changes in key metrics over time.
    - For time-series analysis and identifying seasonality.
    - To measure growth or decline over specific periods.

SQL Functions Used:
    - Date Functions: DATEPART(), DATETRUNC(), FORMAT()
    - Aggregate Functions: SUM(), COUNT(), AVG()
===============================================================================
*/

-- Analyse sales performance over time
-- Quick Date Functions
SELECT
    YEAR(order_date) AS order_year, -- Extracts year from order_date
    MONTH(order_date) AS order_month, -- Extracts month from order_date
    SUM(sales_amount) AS total_sales, -- Sums total sales for the month
    COUNT(DISTINCT customer_key) AS total_customers, -- Counts unique customers
    SUM(quantity) AS total_quantity -- Sums total quantity sold
FROM gold.fact_sales -- Fact table with sales data
WHERE order_date IS NOT NULL -- Exclude null order dates
GROUP BY YEAR(order_date), MONTH(order_date) -- Grouping by year and month
ORDER BY YEAR(order_date), MONTH(order_date); -- Sorting by year and month

-- DATETRUNC() used for cleaner date grouping by month
SELECT
    DATETRUNC(month, order_date) AS order_date, -- Truncates date to the first of each month
    SUM(sales_amount) AS total_sales, -- Total sales for the month
    COUNT(DISTINCT customer_key) AS total_customers, -- Unique customers
    SUM(quantity) AS total_quantity -- Total quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(month, order_date)
ORDER BY DATETRUNC(month, order_date);

-- FORMAT() for user-friendly date format (e.g., 2024-Jan)
SELECT
    FORMAT(order_date, 'yyyy-MMM') AS order_date, -- Format order date
    SUM(sales_amount) AS total_sales, -- Monthly sales
    COUNT(DISTINCT customer_key) AS total_customers, -- Monthly unique customers
    SUM(quantity) AS total_quantity -- Monthly total quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY FORMAT(order_date, 'yyyy-MMM')
ORDER BY FORMAT(order_date, 'yyyy-MMM');

/*
===============================================================================
Cumulative Analysis
===============================================================================
Purpose:
    - To calculate running totals or moving averages for key metrics.
    - To track performance over time cumulatively.
    - Useful for growth analysis or identifying long-term trends.

SQL Functions Used:
    - Window Functions: SUM() OVER(), AVG() OVER()
===============================================================================
*/

-- Calculate total sales and running totals over time
SELECT
    order_date, -- Aggregated to yearly level
    total_sales, -- Yearly total sales
    SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales, -- Running total sales
    AVG(avg_price) OVER (ORDER BY order_date) AS moving_average_price -- Moving average of price
FROM (
    SELECT 
        DATETRUNC(year, order_date) AS order_date, -- Truncate to year
        SUM(sales_amount) AS total_sales, -- Yearly sales total
        AVG(price) AS avg_price -- Average price per year
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY DATETRUNC(year, order_date) -- Grouping by year
) t

/*
===============================================================================
Performance Analysis (Year-over-Year, Month-over-Month)
===============================================================================
Purpose:
    - To measure the performance of products, customers, or regions over time.
    - For benchmarking and identifying high-performing entities.
    - To track yearly trends and growth.

SQL Functions Used:
    - LAG(): Accesses data from previous rows.
    - AVG() OVER(): Computes average values within partitions.
    - CASE: Defines conditional logic for trend analysis.
===============================================================================
*/

-- Analyze yearly product performance
WITH yearly_product_sales AS (
    SELECT
        YEAR(f.order_date) AS order_year, -- Extract year
        p.product_name, -- Product
        SUM(f.sales_amount) AS current_sales -- Yearly sales
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p ON f.product_key = p.product_key
    WHERE f.order_date IS NOT NULL
    GROUP BY YEAR(f.order_date), p.product_name
)
SELECT
    order_year, -- Year
    product_name, -- Product
    current_sales, -- Sales in that year
    AVG(current_sales) OVER (PARTITION BY product_name) AS avg_sales, -- Avg yearly sales
    current_sales - AVG(current_sales) OVER (PARTITION BY product_name) AS diff_avg, -- Diff from avg
    CASE -- Segment by above/below avg
        WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) > 0 THEN 'Above Avg'
        WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) < 0 THEN 'Below Avg'
        ELSE 'Avg'
    END AS avg_change,
    LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS py_sales, -- Previous year
    current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS diff_py, -- YoY change
    CASE -- Categorize YoY change
        WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase'
        WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease'
        ELSE 'No Change'
    END AS py_change
FROM yearly_product_sales
ORDER BY product_name, order_year;

/*
===============================================================================
Data Segmentation Analysis
===============================================================================
Purpose:
    - To group data into meaningful categories for targeted insights.
    - For customer segmentation, product categorization, or regional analysis.

SQL Functions Used:
    - CASE: Defines custom segmentation logic.
    - GROUP BY: Groups data into segments.
===============================================================================
*/

-- Segment products into cost brackets
WITH product_segments AS (
    SELECT
        product_key,
        product_name,
        cost,
        CASE 
            WHEN cost < 100 THEN 'Below 100'
            WHEN cost BETWEEN 100 AND 500 THEN '100-500'
            WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
            ELSE 'Above 1000'
        END AS cost_range
    FROM gold.dim_products
)
SELECT 
    cost_range, -- Segment label
    COUNT(product_key) AS total_products -- Count of products per range
FROM product_segments
GROUP BY cost_range
ORDER BY total_products DESC;

-- Segment customers by spending behavior
WITH customer_spending AS (
    SELECT
        c.customer_key, -- Unique customer
        SUM(f.sales_amount) AS total_spending, -- Total spend
        MIN(order_date) AS first_order, -- First purchase
        MAX(order_date) AS last_order, -- Last purchase
        DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan -- Active period in months
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_customers c ON f.customer_key = c.customer_key
    GROUP BY c.customer_key
)
SELECT 
    customer_segment, -- Segment label
    COUNT(customer_key) AS total_customers -- Count in each segment
FROM (
    SELECT 
        customer_key,
        CASE 
            WHEN lifespan >= 12 AND total_spending > 5000 THEN 'VIP'
            WHEN lifespan >= 12 AND total_spending <= 5000 THEN 'Regular'
            ELSE 'New'
        END AS customer_segment
    FROM customer_spending
) AS segmented_customers
GROUP BY customer_segment
ORDER BY total_customers DESC;
