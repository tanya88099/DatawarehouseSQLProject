
# 🔰 Data Warehouse Setup Using Medallion Architecture

Welcome!  
In this walkthrough, we'll build a **Data Warehouse** using the **Medallion Architecture**, which organizes data into three key layers: **Bronze, Silver, and Gold**.  
Each layer has a specific role in the data journey — from raw ingestion to business-ready analytics.

---

## 🔷 What is the Medallion Architecture?

| Layer      | Purpose                                                                 |
|------------|-------------------------------------------------------------------------|
| **Bronze** | Stores **raw data** as-is from the source. First touchpoint in the pipeline. |
| **Silver** | Holds **cleaned and transformed** data. It refines raw data for easier analysis. |
| **Gold**   | Final, **business-ready data**. Contains **Fact and Dimension tables** using a **Star Schema** model. |

---

## ✅ Step-by-Step Process

---

### 🧱 1. Setting Up the Database

1. Open **SQL Server Management Studio (SSMS)**.
2. Run the following SQL commands to create and switch to the new database:

```sql
USE master;
GO

CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO
```

---

### 📥 2. Load Raw Data into Bronze Layer

This layer will hold the raw unmodified data.

#### Steps:

1. Open and run the script from `DDL_Bronze.sql` to create all necessary Bronze tables.
2. Open and run the stored procedure script from `Proc_load_to_bronze.sql`.  
   This stored procedure handles **bulk insert** of CSV files from your dataset folder into the Bronze layer.

> 💡 **Note:** Ensure your dataset CSV files are placed correctly and paths are accessible from SQL Server for bulk insert to work.

---

### 🧹 3. Clean and Transform Data into Silver Layer

This is the **refined layer**, where data quality improves.

#### Steps:

1. First, open and **review** the stored procedure in `Proc_load_to_silver.sql`. Understand how transformations and cleaning are handled.
2. Run the script `DDL_Silver.sql` to create Silver layer tables.
3. Then, run `Proc_load_to_silver.sql` to execute transformations and populate the Silver tables from the Bronze layer.

> ⚙️ **What happens here?**  
> - NULLs are handled  
> - Invalid values are cleaned  
> - Table structures are standardized  
> - Data types are aligned

---

### 🌟 4. Create Fact & Dimension Views in Gold Layer

This is the final **analytical layer**, where cleaned data is reshaped into **Fact and Dimension views** following the **Star Schema**.

#### Steps:

1. Open and run the `DDL_Gold.sql` script.
2. This script does the following:
   - Joins necessary Silver tables
   - Creates **surrogate keys** using `ROW_NUMBER()` to uniquely identify each record
   - Builds views like `dim_customers`, `dim_products`, and `fact_sales`

> ✅ These Gold views are now ready for reporting, dashboarding, and deep analytics.

---

## 🎯 Final Thoughts

By following this process, you’ve:
- Structured raw data (Bronze)
- Cleaned and standardized it (Silver)
- Shaped it for business reporting (Gold)

This architecture ensures **modularity, scalability, and high-quality analytics**.
