Sales Analytics Data Platform
Business Problem

This project implements an end-to-end batch data pipeline for retail analytics.
The goal is to ingest transactional sales data, transform it into analytics-ready tables, and support historical analysis of customer and product performance.

The platform is designed to reflect real-world data engineering patterns, including raw data ingestion, staged transformations, dimensional modeling, and historical tracking of changes.

Data Pipeline Flow
Source → Raw → Staging → Analytics

Source

CSV files representing retail entities:

customers

customer updates

products

stores

sales transactions

Raw

Data is stored as-is

No transformations applied

Preserves source fidelity for auditing and reprocessing

Staging

Data is cleaned and typed

Invalid or inconsistent records are handled

Business keys are validated

Prepared for analytical modeling

Analytics

Data is modeled into a star schema

Optimized for analytical queries and reporting

Data Model
Fact Table

fact_sales

sale_id

customer_id

product_id

store_id

sale_timestamp

quantity

revenue

Dimension Tables

dim_customer (Slowly Changing Dimension Type 2)

dim_product

dim_store

dim_date

Slowly Changing Dimension (SCD Type 2)

Customer attributes such as email and loyalty tier can change over time.
To preserve historical accuracy, the dim_customer table implements SCD Type 2 using:

customer_id (business key)

effective_start_date

effective_end_date

is_current

When a customer attribute changes, a new row is created while the previous version is expired. This allows accurate historical reporting without overwriting past data.

Repository Structure
Sales-Analytics-Data-Platform/
├── data/
│   └── raw/
├── scripts/
│   └── generate_data.py
├── src/
│   ├── ingest/
│   ├── transform/
│   └── load/
├── sql/
├── airflow/
│   └── dags/
├── README.md
├── requirements.txt
└── .env.example

How to Run
1. Create and activate a virtual environment
python -m venv .venv
source .venv/Scripts/activate

2. Install dependencies
pip install -r requirements.txt

3. Generate synthetic source data
python scripts/generate_data.py


This will create CSV files in data/raw/.

4. Run the pipeline

Pipeline execution scripts will:

ingest raw data

apply staging transformations

build analytics tables

(Execution details will be expanded as the pipeline is finalized.)

Notes

The dataset is synthetic by design to allow controlled simulation of customer changes for SCD Type 2.

The focus of this project is data engineering architecture and transformation logic, not visualization.