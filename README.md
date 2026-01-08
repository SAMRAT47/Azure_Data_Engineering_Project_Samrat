# 🎵 Spotify End-to-End Azure Data Engineering Project

A production-grade Data Engineering solution implementing a Medallion Architecture (Bronze, Silver, Gold) on Azure. This project demonstrates an end-to-end pipeline from data ingestion to serving, featuring incremental loading, backfilling, schema evolution, and CI/CD best practices.


## 📖 Project Overview
This project builds a robust data pipeline to analyze Spotify data. It covers the complete lifecycle of data engineering:
1. Ingestion: Dynamically moving data from Azure SQL Database to Azure Data Lake Gen2 using Azure Data Factory (ADF).
2. Transformation: Processing data using Azure Databricks, Spark Structured Streaming, and Autoloader.
3. Serving: Implementing a Star Schema and Slowly Changing Dimensions (SCD) Type 2 using Delta Live Tables (DLT) and Lakeflow declarative pipelines.
4. Deployment & Monitoring: Utilizing Databricks Asset Bundles (DABs) for CI/CD and Logic Apps for alerting.

The project moves beyond basic ETL by incorporating advanced features like **Unity Catalog** for governance, **Delta Live Tables (DLT)** for declarative pipelines, **Spark Streaming**, and **Databricks Asset Bundles (DABs)** for CI/CD.

## 🛠️ Tech Stack
• Cloud Platform: Microsoft Azure
• Orchestration & Ingestion: Azure Data Factory (ADF)
• Storage: Azure Data Lake Storage (ADLS) Gen2
• Compute & Transformation: Azure Databricks (Spark Core / PySpark)
• Catalog & Governance: Unity Catalog
• Data Modeling: Delta Live Tables (DLT) & Lakeflow
• Version Control: GitHub
• CI/CD: Databricks Asset Bundles (DABs)
• Monitoring: Azure Logic Apps.

---

## 🏗️ Architecture & Workflow

🏗️ Architecture
The project follows the Medallion Architecture:
1. Source: Azure SQL Database (simulating a cloud-hosted production database).
2. Bronze Layer (Raw): Data is ingested into ADLS Gen2 in Parquet format.
    ◦ Key Feature: Dynamic, metadata-driven pipelines in ADF handling incremental loads and backfilling capabilities,.
3. Silver Layer (Enriched): Data is cleaned, deduplicated, and transformed using Databricks.
    ◦ Key Feature: Uses Autoloader (cloudFiles) for schema evolution and Jinja templating for metadata-driven transformations,.
4. Gold Layer (Curated): Final business-level aggregates and dimensions.
    ◦ Key Feature: Implements SCD Type 2 (Auto CDC) for dimensions (dim_user, dim_artist) using Delta Live Tables,.

![Architecture Diagram](https://github.com/SAMRAT47/Azure_Data_Engineering_Project_Samrat/blob/main/Azure_Data_Engineering_Project_Samrat/images/architechture.png)
*Figure 1: High-level End-to-End Architecture Flow.*

### 1. Azure Infrastructure Setup
The project is hosted on a comprehensive Azure Resource Group containing the Data Factory, Databricks Workspace, SQL Database, and Storage Accounts.

![Resource Group](https://github.com/SAMRAT47/Azure_Data_Engineering_Project_Samrat/blob/main/Azure_Data_Engineering_Project_Samrat/images/Resource_Group.PNG)
*Figure 2: The Azure Resource Group containing all provisioned services.*

### 2. Orchestration & Incremental Loading (ADF)
The core orchestration is handled by Azure Data Factory, which manages the **Change Data Capture (CDC)** logic to ensure only new or modified data is processed.

#### **Master Pipeline & Alerting**
The pipeline iterates through table lists using a `ForEach` activity. If the pipeline succeeds or fails, a **Web Activity** triggers an Azure Logic App to send email alerts.

![Main Pipeline](https://github.com/SAMRAT47/Azure_Data_Engineering_Project_Samrat/blob/main/Azure_Data_Engineering_Project_Samrat/images/adf_pipeline_1.PNG)
*Figure 3: The Master Orchestration pipeline with ForEach iterator and Alerting mechanisms.*

#### **Incremental Logic (CDC)**
Inside the iterator, the pipeline performs a **Watermark Lookup**:
1.  **Lookup Old Watermark**: Fetches the last processed timestamp from a control table.
2.  **Get New Watermark**: Fetches the current max timestamp from the source.
3.  **Copy Data**: Moves only the data between `Last_Modified_Date > Old_Watermark` and `Last_Modified_Date <= New_Watermark`.

![Incremental Logic](https://github.com/SAMRAT47/Azure_Data_Engineering_Project_Samrat/blob/main/Azure_Data_Engineering_Project_Samrat/images/adf_pipeline_2.PNG)
*Figure 4: The internal logic for Incremental Data Loading.*

#### **State Management**
Once the data copy is successful, a stored procedure or script updates the control table with the new "High Watermark" to prepare for the next run.

![CDC Update](https://github.com/SAMRAT47/Azure_Data_Engineering_Project_Samrat/blob/main/Azure_Data_Engineering_Project_Samrat/images/adf_pipeline_3.PNG)
*Figure 5: Updating the Watermark table to maintain state.*

---

## 🔑 Key Features Implementation

1. Dynamic Ingestion (Bronze Layer)
• Incremental Loading: Instead of full loads, the pipeline tracks the Last_CDC value using a watermark stored in a JSON file in the Data Lake. It only queries data updated after the last run.
• Dynamic Parameters: A single ADF pipeline handles multiple tables (dim_user, dim_artist, fact_stream) by iterating through a parameter array.
• Backfilling Logic: The pipeline includes specific logic to handle "backdated refreshes," allowing re-processing of historical data dynamically without changing the code.
2. Advanced Transformations (Silver Layer)
• Autoloader: Utilizes Spark Structured Streaming with cloudFiles format to handle new data files efficiently with "exactly-once" processing guarantees (idempotency).
• Schema Evolution: Configured to rescue unexpected columns, ensuring pipeline stability even if source schema changes.
• Metadata-Driven Code: Incorporates Jinja 2 templating to dynamically generate SQL queries for joins and transformations based on parameter dictionaries, reducing boilerplate code.
3. Dimensional Modeling (Gold Layer)
• SCD Type 2: Uses Delta Live Tables (DLT) APPLY CHANGES INTO (Auto CDC) to automatically handle historical tracking for dimension tables based on sequence_by columns.
• Quality Expectations: Implements DLT Expectations (e.g., @dlt.expect_or_drop) to enforce data quality rules (e.g., checking for null IDs) before data enters the Gold layer.
4. CI/CD & Operations
• Databricks Asset Bundles (DABs): Defines the project infrastructure and code as code (databricks.yml), enabling deployment across Dev, QA, and Prod environments via CLI.
• Alerting: Configured Azure Logic Apps to send email notifications via a webhook triggered by pipeline failures in ADF.

## 🚀 How to Run
1.  **Clone the Repo**:
    ```bash
    git clone [https://github.com/SAMRAT47/Azure_Data_Engineering_Project_Samrat.git](https://github.com/SAMRAT47/Azure_Data_Engineering_Project_Samrat.git)
    ```
2.  **Infrastructure**: Provision the resources shown in Figure 1 using the Azure Portal or ARM templates.
3.  **ADF Setup**: Import the pipelines from the `adf/` folder into your Data Factory instance.
4.  **Databricks**:
    * Install the Databricks CLI.
    * Deploy the bundle: `databricks bundle deploy -t dev`
5.  **Database**: Run the SQL scripts in `sql_scripts/` to create the source tables and watermark control table.

## 📬 Contact
**Author**: Samrat Roychoudhury
* **GitHub**: [SAMRAT47](https://github.com/SAMRAT47)
* **Email**: samrat.rc47@gmail.com

---
*Created as part of an extensive study on Modern Azure Data Engineering.*
