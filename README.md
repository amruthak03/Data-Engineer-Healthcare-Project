# Healthcare RCM Data Lake

A comprehensive data engineering solution for Revenue Cycle Management (RCM) in healthcare, built on Google Cloud Platform. This project demonstrates enterprise-grade data lake architecture, ETL orchestration, and analytics capabilities for processing multi-source healthcare data.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Key Features](#key-features)
- [Data Sources](#data-sources)
- [Project Structure](#project-structure)
- [Setup & Installation](#setup--installation)
- [Pipeline Execution](#pipeline-execution)
- [Business Analytics](#business-analytics)
- [Future Enhancements](#future-enhancements)

## 🎯 Overview

Healthcare RCM Data Lake centralizes, cleanses, and transforms healthcare data from multiple sources, enabling healthcare providers and insurance companies to streamline billing, claims processing, and revenue tracking. The solution processes Electronic Medical Records (EMR), insurance claims, and standardized medical codes to deliver actionable business insights. 

**Note**: This project simulates real-world healthcare data sources using synthetic data and applies industry best practices including metadata-driven pipelines, Slowly Changing Dimensions (SCD Type 2), Common Data Model (CDM), Medallion Architecture, orchestration with Apache Airflow, and CI/CD automation.

**Project Goals:**
- Build a scalable, cloud-native data lake on GCP
- Implement medallion architecture (Bronze → Silver → Gold layers)
- Enable automated ETL workflows with metadata-driven approach
- Provide analytics-ready datasets for revenue cycle management
- Demonstrate enterprise data engineering best practices in healthcare domain
- Showcase end-to-end pipeline orchestration and CI/CD implementation

## 🏗️ Architecture

The project implements a modern data lake architecture with three processing layers:

### Medallion Architecture
```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│   Bronze    │────▶│    Silver    │────▶│    Gold     │
│  (Raw Data) │     │  (Cleansed)  │     │ (Analytics) │
└─────────────┘     └──────────────┘     └─────────────┘
```
- **Bronze Layer**: Raw, unprocessed data from all sources
- **Silver Layer**: Cleansed, transformed, and enriched data with Common Data Model (CDM)
- **Gold Layer**: Business-ready aggregated datasets for analytics and reporting

### Workflow Overview

1. **Data Ingestion**: PySpark jobs extract data from SQL databases and flat files
2. **Landing Zone**: Raw data stored in GCS bucket folders
3. **Bronze Layer**: External tables created in BigQuery pointing to raw data
4. **Silver Layer**: Data transformation, standardization, and SCD Type 2 implementation
5. **Gold Layer**: Business analytics tables with KPIs and insights
6. **Orchestration**: Airflow DAGs manage end-to-end pipeline execution
7. **CI/CD**: GitHub + Cloud Build automates deployment

## 🛠️ Tech Stack

| Component | Technology |
|-----------|-----------|
| **Cloud Platform** | Google Cloud Platform (GCP) |
| **Data Storage** | Google Cloud Storage (GCS) |
| **Data Warehouse** | BigQuery |
| **Processing Engine** | Dataproc (Apache Spark) |
| **Orchestration** | Cloud Composer (Apache Airflow) |
| **Source Database** | Cloud SQL (MySQL) |
| **Version Control** | GitHub |
| **CI/CD** | Cloud Build |
| **Languages** | Python, PySpark, SQL |

## ✨ Key Features

### 1. Metadata-Driven ETL
- Configuration-based pipeline design using `load_config.csv`
- Dynamic job execution without hardcoded logic
- Easy scaling for new data sources

### 2. Slowly Changing Dimensions (SCD Type 2)
- Historical tracking of patient demographics, provider details, and transactions
- Maintains complete audit trail with effective dates
- Supports temporal queries and trend analysis

### 3. Common Data Model (CDM)
- Standardized schema across multiple hospitals
- Ensures data consistency and interoperability
- Simplifies cross-facility analytics

### 4. Medallion Architecture
- **Bronze**: Preserves raw data lineage
- **Silver**: Implements business logic and data quality rules
- **Gold**: Delivers pre-aggregated analytics tables

### 5. Comprehensive Logging & Monitoring
- **Audit Logs**: Track pipeline execution, record counts, and timestamps
- **Pipeline Logs**: Capture job-level metrics and errors
- Real-time monitoring through Cloud Composer UI

### 6. Robust Error Handling
- Data validation at each layer
- Exception logging to BigQuery tables
- Automated retry mechanisms in Airflow

### 7. CI/CD Pipeline
- GitHub integration for version control
- Cloud Build triggers on code commits
- Automated DAG synchronization to Composer

## 📊 Data Sources

### 1. Electronic Medical Records (EMR) - Cloud SQL
Two hospital databases with identical schemas:

**Hospital A & Hospital B Databases:**
- `patients` - Patient demographics
- `providers` - Healthcare provider information
- `encounters` - Patient visit records
- `transactions` - Billing and payment transactions
- `departments` - Hospital department details

### 2. Claims Data
- Source: Insurance companies
- Format: CSV files (monthly batches)
- Coverage: Separate files for Hospital A and Hospital B

### 3. CPT Codes (Current Procedural Terminology)
- Source: Public API / Reference data
- Format: CSV file
- Purpose: Standardized medical procedure descriptions

## 📁 Project Structure
```
healthcare-rcm-data-lake/
│
├── workflows/
│   ├── parent_dag.py              # Master orchestration DAG
│   ├── pyspark_dag.py             # Ingestion pipeline DAG
│   └── bq_dag.py                  # BigQuery transformation DAG
│
├── data/INGESTION/
│        ├── hospitalA_mysqlToLanding.py
│        ├── hospitalB_mysqlToLanding.py
│        ├── claims.py
│        └── cpt_codes.py
│
├── data/BQ/
│        ├── bronze.sql            # External table creation
│        ├── silver.sql            # Transformation & CDM
│        └── gold.sql              # Business analytics queries
│
├── configs/
│   └── load_config.csv            # Metadata configuration
│
├── cloudbuild.yaml                # CI/CD pipeline definition
├── utils
│   ├── add_dags_to_composer.py              
│   └── requirements.txt           # Python dependencies
└── README.md                      # Project documentation
```

### GCS Bucket Structure
```
healthcare-project/
├── configs/
│   └── load_config.csv
├── data/
│   ├── hosp-a/                    # Hospital A source CSVs
│   └── hosp-b/                    # Hospital B source CSVs
├── landing/
│   ├── hospital-a/                # Extracted EMR data
│   ├── hospital-b/
│   ├── claims/
│   └── cptcodes/
└── temp/
    └── pipeline_logs/
```

### BigQuery Dataset Structure
```
bigquery_project/
├── temp_dataset/
│   ├── audit_log
│   └── pipeline_logs
├── bronze_dataset/
│   ├── patients_ha, patients_hb
│   ├── providers_ha, providers_hb
│   ├── encounters_ha, encounters_hb
│   ├── transactions_ha, transactions_hb
│   ├── departments_ha, departments_hb
│   ├── claims
│   └── cpt_codes
├── silver_dataset/
│   ├── patients
│   ├── providers
│   ├── encounters
│   ├── transactions
│   └── departments
└── gold_dataset/
    ├── patient_history
    ├── department_performance
    ├── provider_charge_summary
    ├── provider_performance
    ├── financial_kpis
    ├── financial_metrics_by_month
    └── payor_performance
```

## 🚀 Setup & Installation

### Prerequisites

- Google Cloud Platform account with billing enabled
- GCP Project with required APIs enabled:
  - Cloud Storage API
  - BigQuery API
  - Dataproc API
  - Cloud Composer API
  - Cloud SQL Admin API
  - Cloud Build API
- GitHub account
- `git` CLI installed and configured

#### 5. GitHub & CI/CD Setup
```bash
# Clone repository
git clone https://github.com/your-username/healthcare-rcm-data-lake.git
cd healthcare-rcm-data-lake

# Connect Cloud Build to GitHub
# (Follow Cloud Build console UI to authorize GitHub)

# Create Cloud Build trigger
gcloud builds triggers create github \
    --repo-name=healthcare-rcm-data-lake \
    --repo-owner=your-username \
    --branch-pattern=^main$ \
    --build-config=cloudbuild.yaml
```

## ▶️ Pipeline Execution

### Manual Execution

1. **Run Ingestion Pipeline** (via Dataproc Jupyter):
   - Upload PySpark notebooks to cluster
   - Execute `hospitalA_mysqlToLanding.py`
   - Execute `hospitalB_mysqlToLanding.py`
   - Execute `claims.py`
   - Execute `cpt_codes.py`

2. **Create Bronze Tables** (via BigQuery):
```sql
   -- Execute bronze.sql
```

3. **Run Transformation** (via BigQuery):
```sql
   -- Execute silver.sql
   -- Execute gold.sql
```

### Automated Execution (Airflow)

1. Access Cloud Composer Airflow UI
2. Locate `parent_workflow` DAG
3. Trigger DAG manually or wait for scheduled run
4. Monitor execution:
   - `pyspark_workflow` → Ingestion jobs
   - `bigquery_workflow` → Transformation jobs

### Pipeline Workflow
```
parent_workflow
│
├── pyspark_workflow
│   ├── start_cluster
│   ├── hospitalA_ingestion
│   ├── hospitalB_ingestion
│   ├── claims_ingestion
│   ├── cpt_codes_ingestion
│   └── stop_cluster
│
└── bigquery_workflow
    ├── create_bronze_tables
    ├── create_silver_tables
    └── create_gold_tables
```

## 📈 Business Analytics

The Gold layer contains 7 pre-built analytics tables answering critical business questions:
### 1. Patient History Summary
Provides a comprehensive view of each patient's healthcare journey, consolidating all encounters, diagnoses, billing amounts, insurance claims, and out-of-pocket payments. This enables patient account representatives to quickly access complete financial histories, identify outstanding balances, and understand claim patterns for individual patients.

### 2. Provider Charge Summary
Aggregates total charges generated by each healthcare provider across different departments, revealing which providers are driving the most revenue and how their billing patterns vary by specialty or department. This supports resource allocation decisions and helps identify high-performing providers.

### 3. Provider Performance Summary
Delivers a comprehensive performance scorecard for each provider, tracking the number of patient encounters, total billed amounts, claim submission rates, and claim success rates. This metric helps identify providers who may need additional training on proper coding or documentation, and recognizes top performers in revenue generation and claim approval.

### 4. Department Performance Analytics
Provides department-level insights into operational efficiency, revenue generation, patient volume, and resource utilization. Leaders can identify which departments are most profitable, which have the highest patient throughput, and where operational bottlenecks may exist. This supports strategic planning for capacity expansion or resource reallocation.

### 5. Financial KPIs (Key Performance Indicators)
Calculates organization-wide financial health metrics including total revenue, total claims submitted and approved, overall claim approval rates, average transaction values, and revenue per encounter. These executive-level KPIs provide a snapshot of the organization's financial performance and help track progress toward revenue goals.

### 6. Financial Metrics by Month
Enables time-series analysis of revenue trends, showing how billing, collections, claim approvals, and patient volumes fluctuate month-over-month. This temporal view helps identify seasonal patterns, forecast future revenue, detect anomalies, and measure the impact of operational changes or policy updates on financial performance.

### 7. Payor Performance & Claims Summary
Analyzes insurance company (payor) performance by tracking claim submission volumes, approval rates, denial reasons, and average reimbursement amounts for each insurance provider. This intelligence helps negotiate better contracts with payors, identify problematic insurance relationships, and optimize claim submission strategies to maximize approval rates.

### Analytics Use Cases

These gold-layer analytics support multiple business functions:

- **Revenue Cycle Teams**: Identify claim denial patterns and optimize reimbursement strategies
- **Finance Leadership**: Monitor cash flow, forecast revenue, and track key performance metrics
- **Clinical Operations**: Understand provider productivity and department utilization
- **Executive Management**: Make strategic decisions based on comprehensive financial and operational insights
- **Compliance Teams**: Audit billing practices and ensure proper documentation and coding

## 🔮 Future Enhancements

- [ ] **Real-time Streaming**: Implement Dataflow for near real-time claims processing
- [ ] **ML Integration**: Predict claim denials using Vertex AI
- [ ] **Data Catalog**: Implement metadata management with Data Catalog
- [ ] **BI Dashboard**: Build Looker Studio dashboards for executives

---

⭐ **Star this repository** if you found it helpful!
