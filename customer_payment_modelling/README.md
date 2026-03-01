# payment_customer_dbt_modelling

This dbt project transforms, models, and analyzes payment and customer data to generate insights. It utilizes DuckDB as the data warehouse.

## Project Overview

This project follows a standard dbt workflow, progressing from raw CSV data to a curated data warehouse ready for analysis. The project structure is organized into seeds, staging, warehouse, and analysis folders, each serving a specific purpose.

## Data Sources

4 CVS files were loaded into the `seeds` folder for initial ingestion into DuckDB.

## Data Loading and Transformation

1.  **Seeds:** The CSV files were initially loaded into DuckDB using dbt seeds. However, the `payment_request_data.csv` file encountered issues during loading due to inconsistencies in the JSON format within the `metadata` column.

2.  **Staging:** To address the JSON format issues in `payment_request_data.csv`, transformations were performed in the `staging` folder. This involved cleaning and standardizing the JSON data within the `metadata` column, enabling successful loading into DuckDB.  Additionally, minor transformations were applied to other columns as needed.  This staging layer prepares the data for integration into the data warehouse.

3. **Warehouse:** The `warehouse` folder contains the core logic for building the final data warehouse tables. data types are explicitly defined, and final transformations are applied to prepare the data for analysis. This is where the fact table and dimension tables are built and additional two tables from 'customer details' field.  Deduplication logic is implemented in this layer to ensure data quality.

4. **Analyses:** The `analyses` folder contains SQL queries used to generate insights based on the business questions. These queries leverage the curated data in the warehouse to answer specific analytical questions.

## Running the project

To run this project, you will need to:
1. Install dbt and DuckDB.
2. Confiqure your `profile.yml` file to connect to DuckDB instance.
3. Run the following dbt commands:
  ```
  dbt seed: Load the CSV data into DuckDB
  dbt run:  Run the dbt models
  dbt test: Run the dbt tests
  ```
  
## Conclution
This project provides a comprehensive analysis of the payment data. It addresses data quality challenges, implements best practices for data warehousing, and delivers insights to business questions.