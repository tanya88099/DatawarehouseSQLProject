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
