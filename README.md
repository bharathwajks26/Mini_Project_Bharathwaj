# 🏢 Enterprise Employee Data ETL Pipeline

A scalable medallion architecture ETL pipeline built on **Databricks** and **AWS S3**. This project ingests raw employee data, applies data quality rules, implements SCD Type 2 for historical tracking, and publishes a final analytics-ready dataset.

## 🏗️ Architecture Overview

This pipeline follows the **Medallion Architecture** (Bronze → Silver → Gold) pattern to ensure data quality, traceability, and historical accuracy.

```mermaid
graph LR
    A[AWS S3 Source] -->|Ingestion| B(BRONZE / RAWZ)
    B -->|Cleanse & Validate| C(SILVER / WORK)
    C -->|SCD Type 2 Merge| D(GOLD / CONFZ)
    D -->|Star Schema Join| E(PUBZ / RESULT)
    
    style B fill:#f9f,stroke:#333,stroke-width:2px
    style C fill:#bbf,stroke:#333,stroke-width:2px
    style D fill:#bfb,stroke:#333,stroke-width:2px
    style E fill:#fbb,stroke:#333,stroke-width:2px
