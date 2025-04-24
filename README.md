# Data Warehouse and Analytics Project 🚀
This project demonstrates a comprehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights. Designed as a portfolio project, it highlights industry best practices in data engineering and analytics.

🏗️ Data Architecture
The data architecture for this project follows Medallion Architecture Bronze, Silver, and Gold layers:
![image](https://github.com/user-attachments/assets/82a12f35-c632-4b9b-8c88-1f138bbd8923)


# 📊 Sales Analytics SQL Project

## 📝 Overview

This project is a comprehensive SQL-based data analysis pipeline that transforms raw sales data into actionable insights. It covers everything from data extraction, transformation, and loading (ETL), to building advanced KPIs and reporting views. The project showcases my ability to perform analytical reporting, segmentation, time-series analysis, and customer/product behavior analysis using SQL.

---

## 🔁 ETL Process

**1. Data Loading**

- Sales data is stored in a fact table: `gold.fact_sales`
- Supporting dimension tables:
  - `gold.dim_products`
  - `gold.dim_customers`

These tables are assumed to be pre-loaded into the `gold` schema via upstream ETL pipelines (not covered in this script).

---

## 📈 Analytical Use Cases Implemented

### 1. 🔄 Change Over Time Analysis

**Goal:**  
Track trends, growth, and changes in key sales metrics across time.

**Techniques Used:**
- `DATEPART`, `DATETRUNC`, and `FORMAT` for flexible time bucketing
- Aggregation with `SUM`, `COUNT`, `AVG`
- Monthly and yearly trend analysis

### 2. 📊 Cumulative Analysis

**Goal:**  
Track cumulative and moving metrics over time for growth analysis.

**Techniques Used:**
- Subqueries for monthly aggregation
- `SUM() OVER()` for running totals
- `AVG() OVER()` for moving average of sales price

### 3. 📆 Performance Analysis (YoY / MoM)

**Goal:**  
Evaluate performance year-over-year for each product.

**Techniques Used:**
- `LAG()` for prior year comparisons
- `AVG() OVER(PARTITION BY)` for benchmarking
- `CASE` statements to determine trends (e.g., Above/Below Avg)

### 4. 📂 Data Segmentation Analysis

**Goal:**  
Segment products and customers into meaningful buckets.

**Product Segmentation:**
- By cost ranges: `<100`, `100–500`, `500–1000`, `>1000`

**Customer Segmentation:**
- Based on lifespan and total spend:
  - **VIP**: Lifespan ≥ 12 months & spend > €5,000
  - **Regular**: Lifespan ≥ 12 months & spend ≤ €5,000
  - **New**: Lifespan < 12 months

### 5. 🧩 Part-to-Whole Analysis

**Goal:**  
Evaluate the contribution of each category to overall sales.

**Techniques Used:**
- `SUM() OVER()` for overall sales
- Percentage contribution calculation
- Ranking by sales performance

---

## 📚 Reporting Views Created

### 1. 👥 Customer Report: `gold.report_customers`

**Features:**
- Combines customer demographics and behavior
- Segments customers by:
  - **Age Group**: Under 20, 20–29, ..., 50+
  - **Engagement Type**: VIP, Regular, New
- KPIs Calculated:
  - Total Orders, Sales, Products, Quantity
  - Lifespan, Recency (months since last order)
  - **AOV** (Average Order Value)
  - **AMS** (Average Monthly Spend)

### 2. 📦 Product Report: `gold.report_products`

**Features:**
- Categorizes products by performance:
  - **High Performer**, **Mid-Range**, **Low Performer**
- KPIs Calculated:
  - Total Sales, Orders, Customers, Quantity
  - Recency (since last sale), Lifespan
  - **AOR** (Average Order Revenue)
  - **AMR** (Average Monthly Revenue)
  - Avg. Selling Price per Product

---

## 🛠️ SQL Techniques & Functions Used

- **Window Functions:** `SUM() OVER()`, `LAG()`, `AVG() OVER()`
- **Date Functions:** `DATETRUNC()`, `FORMAT()`, `DATEDIFF()`
- **Conditional Logic:** `CASE`
- **Aggregations:** `SUM()`, `COUNT()`, `AVG()`
- **Subqueries & CTEs**: For layered transformations and KPIs
- **View Creation:** `CREATE VIEW` with `IF OBJECT_ID EXISTS` check

---

## 📌 Conclusion

This project demonstrates the ability to:

✅ Perform complete time-series, segmentation, and performance analysis  
✅ Build comprehensive customer and product reporting systems  
✅ Translate raw transactional data into business-ready KPIs  
✅ Apply real-world SQL best practices

---

## 🧠 Author

**Tanya Singh**  
Data Modeler | SQL Enthusiast | Azure Certified  
📬 [Optional: Add LinkedIn or portfolio link here]

