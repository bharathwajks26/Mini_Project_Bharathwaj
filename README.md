# 🏢 Enterprise Employee Data ETL Pipeline

A scalable medallion architecture ETL pipeline built on **Databricks** and **AWS S3**. This project ingests raw employee data, applies data quality rules, implements SCD Type 2 for historical tracking, and publishes a final analytics-ready dataset.
## 🔄 Orchestration

This pipeline is orchestrated using **Databricks Workflows** with **17 tasks** executing in parallel where possible.

### Workflow Graph
<img width="888" height="572" alt="Bharathwaj_DM" src="https://github.com/user-attachments/assets/20a7d41f-0805-4027-8abe-8c1fa5d8f43d" />


### Task Structure
| Layer | Tasks | Execution Mode | Avg Runtime |
|-------|-------|----------------|-------------|
| **Ingestion** | 1 (Ingestion_Util) | Sequential | ~10s |
| **RAWZ** | 5 (Employee, Dept, Project, Training, Payroll) | ✅ Parallel | ~1-2 min |
| **WORK** | 5 notebooks | ✅ Parallel | ~30s |
| **CONFZ** | 5 notebooks | ✅ Parallel | ~30s |
| **PUBZ** | 1 (Result_Pubz) | Sequential (depends on all CONFZ) | ~15s |

### Total Pipeline Runtime
| Metric | Value |
|--------|-------|
| **First Run** | ~5 minutes |
| **Subsequent Runs** | ~3 minutes |
| **Success Rate** | 100% |

### Schedule Configuration

Trigger: Manual (can be scheduled)
Frequency: Daily at 2:00 AM UTC (configurable)
Retry Policy: 2 retries with 5-minute intervals
Notifications: Email alerts on failure

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

