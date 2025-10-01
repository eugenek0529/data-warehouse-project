# Data Warehouse Project
A basic data warehousing project built using PostgreSQL, SQL, and ETL best practices. This project demonstrates a complete data pipeline — from raw data ingestion to delivering actionable insights.

...

The goal of this project is to showcase a **comprehensive data warehousing and analytics solution**, including:

- Building a data warehouse
- Performing ETL processes
- Implementing data modeling techniques
- Delivering clean, business-ready data for analytics

<img width="643" height="415" alt="image" src="https://github.com/user-attachments/assets/c266604b-861c-4192-bb2e-0cef9dd68103" />


This project follows the **Medallion Architecture** pattern:

### 🥉 Bronze Layer: Raw Data

- **Source**: CSV files from CRM & ERP systems
- **Load**: Batch processing, full load (truncate & insert)
- **Transformation**: None (data is ingested as-is)
- **Object Type**: Tables

### 🥈 Silver Layer: Cleaned Data

- **Transformations**:
    - Data Cleaning
    - Standardization
    - Normalization
    - Derived Columns
    - Enrichment
- **Load**: Same as Bronze (batch, full load)
- **Object Type**: Tables

### 🥇 Gold Layer: Business-Ready Data

- **Transformations**:
    - Business Logic
    - Aggregations
    - Data Integration
- **Data Modeling**:
    - Star Schema
    - Flat Tables
    - Aggregated Tables
- **Object Type**: Views


This project is licensed under the MIT License.
