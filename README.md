%md
# 🏦 Banking Lakehouse Implementation on Databricks
### *A Medallion Architecture via Dual-Engine (PySpark & SQL) Development*

<img width="1408" height="598" alt="Screenshot 2026-02-18 134332" src="https://github.com/user-attachments/assets/ab425fad-1422-4932-8f11-3748b19f86d8" />


## 📖 Project Overview
This project establishes a robust, enterprise-grade **Banking Data Lakehouse** designed to process millions of transactions and customer records. The solution follows the **Medallion Architecture**, moving from raw ingestion to business-ready aggregates. 

**Dual-Implementation Approach:** The entire pipeline was successfully developed and tested twice—once using **PySpark** for programmatic flexibility and again using **Declarative SQL** for streamlined maintenance and readability.



---

## 🏗️ Technical Architecture & Data Flow
The pipeline utilizes **Delta Live Tables (DLT)** to manage dependencies and data quality across three distinct layers:

### **1. Landing Layer (The Entry Point)**
* **Technology:** Databricks Auto Loader (`cloud_files`).
* **Function:** Ingests incremental CSV files from Unity Catalog Volumes with automatic schema inference.

### **2. Bronze Layer (Validation & Cleaning)**
* **Logic:** Standardizes casing, cleans phone numbers via Regex, and standardizes date formats.
* **Governance:** Implements **DLT Expectations** (e.g., `EXPECT_OR_FAIL`, `EXPECT_OR_DROP`) to enforce data quality at the gate.

### **3. Silver Layer (Stateful Enrichment)**
* **Feature Engineering:** Derived columns such as `customer_age`, `tenure_days`, and `txn_direction`.
* **State Management:**
    * **SCD Type 1:** Applied to Customers to maintain the most recent profile.
    * **SCD Type 2:** Applied to Transactions to maintain a full historical audit trail.

### **4. Gold Layer (Materialized Views & KPIs)**
* **Materialized Views:** High-performance joins between enriched customer profiles and transaction history.
* **Aggregations:** Computes business metrics including total credits/debits and risk segment profiles.

---

## 🛠️ Multi-Language Implementation
To demonstrate technical proficiency, this project was architected in two versions:

1. **PySpark Version:** Focused on programmatic control, unit testing capability, and complex functional transformations.
2. **Declarative SQL Version:** Focused on readability, performance optimization, and accessibility for data analysts.

---

## 🔄 Orchestration & Visualization
* **Orchestration:** Managed via **Databricks Workflows**, scheduling the DLT pipeline and downstream tasks.
* **Dashboards:** A **Databricks SQL Dashboard** providing real-time visibility into **Customer 360**, **Risk Monitoring**, and **Operational Trends**.


