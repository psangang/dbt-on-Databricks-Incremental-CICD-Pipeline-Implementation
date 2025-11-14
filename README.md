# dbt-on-Databricks-Scalable-Incremental-Pipeline-Implementation
Implementation of incremental data models/ CICD using dbt on Databricks, including schema management, Jinja, macros, and performance-optimized ETL logic for large datasets.

🚀 Modern Data Pipeline using dbt + Databricks
End-to-End Analytics Engineering Project

This project demonstrates how to build a modular, scalable, and production-ready data pipeline using dbt Core with Databricks as the data platform.

It follows the Medallion Architecture — Bronze → Silver → Gold — implementing real-world best practices like incremental models, macros, seeds, and data testing.

🧱 Architecture Overview

Medallion Layers:

🥉 Bronze Layer: Raw data ingestion tables directly loaded from source.
🥈 Silver Layer: Cleaned, deduplicated, and transformed intermediate models.
🥇 Gold Layer: Business-ready aggregated models for analytics and dashboards.

⚙️ Tech Stack
Component	Description
dbt Core	Transformation framework managing SQL logic, lineage, and testing
Databricks SQL Warehouse	Execution engine and data lakehouse platform
Delta Lake	Storage format supporting ACID transactions & incremental updates
Jinja & Macros	For dynamic SQL generation and reusable logic
Seeds	To load small reference datasets into the database
Analyses	Ad-hoc queries and exploration reports
GitHub	Version control and CI/CD integration (optional setup)
🧩 Project Structure
.
├── models/
│   ├── bronze/
│   │   └── bronze_sales.sql
│   ├── silver/
│   │   └── silver_sales_cleaned.sql
│   ├── gold/
│   │   └── gold_sales_summary.sql
│   └── schema.yml
│   ├── source/
│   │   └── sources.yml
│
├── macros/
│   ├── generate_schema.sql
│   └── multiply.sql
│
├── seeds/
│   └── mapping_or_lookup.csv
|
├── snapshots/
│   └── gold_items.yml
|
├── analyses/
│   └── jinja1.sql
|   └── jinja2.sql
|   └── jinja3.sql
|   └── query_macro.sql
|   └── target_variables.sql
│
├── dbt_project.yml
└── README.md

🔁 Incremental Model Example

Example logic for incremental model in silver_sales_cleaned.sql:

{% set inc_flag = 1 %}
{% set cols_list = ['sales_id', 'date_sk', 'gross_amount'] %}

select
    {% for i in cols_list %}
        {{ i }}
        {% if not loop.last %}, {% endif %}
    {% endfor %}
from {{ ref('bronze_sales') }}

{% if inc_flag == 1 %}
    where date_sk > (select max(date_sk) from {{ this }})
{% endif %}

OR

{{ config(materialized='incremental', unique_key='sales_id') }}

select
    sales_id,
    date_sk,
    gross_amount
from {{ ref('bronze_sales') }}

{% if is_incremental() %}
    where date_sk > (select max(date_sk) from {{ this }})
{% endif %}


✅ This ensures only new data since the last load is processed.

🧠 Macros Example

Reusable macro to get the latest date from a target model:

{% macro get_max_date(model) %}
    select max(date_sk) from {{ model }}
{% endmacro %}


Used inside models as:

where date_sk > ({{ get_max_date(this) }})

🧪 Data Testing

Column-level and table-level tests via schema.yml:

version: 2

models:
  - name: silver_sales_cleaned
    description: "Cleaned sales data with deduplication"
    columns:
      - name: sales_id
        tests:
          - unique
          - not_null
      - name: date_sk
        tests:
          - not_null

🌱 Seeds

Lookup data (e.g., country codes or currency conversions) loaded with:

dbt seed

📊 Analyses

Exploratory SQL scripts under /analyses for quick insights and validation.

🚀 How to Run
1️⃣ Setup environment
python -m venv dbt_env
source dbt_env/bin/activate
pip install dbt-databricks

2️⃣ Configure your Databricks profile

Edit your ~/.dbt/profiles.yml:

your_project:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: dbt_psangang_dev
      schema: bronze
      host: <your-databricks-host>
      http_path: <your-sql-warehouse-http-path>
      token: <your-access-token>

3️⃣ Run dbt commands
dbt debug
dbt seed
dbt run
dbt run --select 
dbt test
dbt docs generate
dbt docs serve
dbt clean
dbt build
dbt snapshot --select .\snapshots\gold_items.yml
dbt run --select .\models\gold\source_gold_items.sql
dbt run --select .\models\gold\source_gold_items.sql
dbt build --target prod

📈 Key Learnings

Designing modular Bronze–Silver–Gold pipelines
Implementing incremental models using {{ this }}
Using Jinja + Macros for reusable SQL logic
Ensuring data quality via dbt tests
Running transformations on Databricks Lakehouse

💡 Future Enhancements

CI/CD with GitHub Actions
Historical tracking via Snapshots
dbt Cloud deployment & scheduling
Visualization layer (Tableau / Power BI / Databricks SQL Dashboards)

🧑‍💻 Author

Psangang
Data Engineer & Analytics Enthusiast
