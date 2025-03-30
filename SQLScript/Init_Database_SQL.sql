/*
===============================================================================
Welcome! 
Our first step is to create Database and Schema
===============================================================================
*/
Use master;
/*
===============================================================================
Create Database name DataWarehouse
===============================================================================
*/
create database DataWarehouse;

use DataWarehouse;
/*
===============================================================================
As we are using Medallion Architecture, we need to create three schema: Bronze, Silver, Gold
===============================================================================
*/
create schema bronze;
go 
create schema silver;
go
create schema gold;
