# Azure End-to-End Data Engineering Project 🚀

## 📌 Project Overview
This is an in-depth Data Engineering project that builds a robust, scalable ETL pipeline using the Microsoft Azure ecosystem. It demonstrates a complete lifecycle from data ingestion to processing and serving, leveraging modern data architecture principles.

The project moves beyond basic ETL by incorporating advanced features like **Unity Catalog** for governance, **Delta Live Tables (DLT)** for declarative pipelines, **Spark Streaming**, and **Databricks Asset Bundles (DABs)** for CI/CD.

## 🛠️ Tech Stack
* **Cloud Platform**: Microsoft Azure
* **Orchestration**: Azure Data Factory (ADF)
* **Compute & Processing**: Azure Databricks (PySpark, Spark Streaming)
* **Storage**: Azure Data Lake Storage Gen2 (ADLS), Azure SQL Database
* **Data Governance**: Unity Catalog
* **Pipeline Management**: Delta Live Tables (DLT)
* **DevOps**: Databricks Asset Bundles (DABs), GitHub Actions
* **Alerting**: Logic Apps (Web Activity)

---

## 🏗️ Architecture & Workflow
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

## 🔑 Key Features
* **Databricks Asset Bundles (DABs)**: The entire Databricks workflow is defined as code (`databricks.yml`), enabling professional CI/CD and deployment across Dev/Prod environments.
* **Delta Live Tables (DLT)**: utilized for building reliable and maintainable data pipelines with built-in quality controls (expectations).
* **Unity Catalog**: Implemented for centralized access control and data discovery across the workspace.
* **Spark Streaming**: utilized for near real-time data ingestion and processing.

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
