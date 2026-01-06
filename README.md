# Optimizing-Aviation-Insights
A Data Engineering Approach for Analyzing Flight Delays and Cancellations A Data Engineering Case Study
✈️ Flight Delay & Cancellation Analysis
PySpark Medallion Architecture (Batch + Streaming)
📌 Overview

This project implements a scalable data engineering pipeline to analyze U.S. flight delays and cancellations using PySpark and Azure Data Lake Storage (ADLS).
The solution follows the Medallion Architecture (Bronze–Silver–Gold) pattern and processes both historical batch data and real-time streaming data, without using Azure Data Factory.

The pipeline enables efficient ingestion, cleansing, integration, and aggregation of flight data, delivering analytics-ready datasets for BI and advanced analytics.

🗂️ Dataset Description
File Name	Type	Description
airlines.csv	Batch	Airline master data
airports.csv	Batch	Airport master data
flights_old.csv	Batch	Historical flight data (2015)
flights_latest.csv	Streaming	Incremental flight data
🏗️ Architecture Overview
🔹 Medallion Architecture
                   ┌──────────────────────┐
                   │   Raw CSV Sources    │
                   │──────────────────────│
                   │ airlines.csv         │
                   │ airports.csv         │
                   │ flights_old.csv      │
                   │ flights_latest/      │
                   └─────────┬────────────┘
                             │
                             ▼
                   ┌──────────────────────┐
                   │      BRONZE LAYER    │
                   │──────────────────────│
                   │ Raw ingestion        │
                   │ Batch + Streaming    │
                   │ Parquet format       │
                   └─────────┬────────────┘
                             │
                             ▼
                   ┌──────────────────────┐
                   │      SILVER LAYER    │
                   │──────────────────────│
                   │ Data cleaning        │
                   │ Schema standardization│
                   │ Feature engineering  │
                   │ Dimensional modeling │
                   └─────────┬────────────┘
                             │
                             ▼
                   ┌──────────────────────┐
                   │       GOLD LAYER     │
                   │──────────────────────│
                   │ Aggregated datasets  │
                   │ BI & Analytics ready │
                   └──────────────────────┘

📂 Data Lake Structure
adls/
│
├── bronze/
│   ├── airlines/
│   ├── airports/
│   ├── flights_old/
│   └── flights_latest_stream/
│
├── silver/
│   ├── dim_airlines/
│   ├── dim_airports/
│   └── fact_flights/
│
└── gold/
    ├── airline_summary/
    ├── airport_summary/
    └── monthly_trends/

🔄 Data Processing Flow
🟫 Bronze Layer – Raw Data Ingestion

Batch ingestion of airlines, airports, and flights_old

Structured Streaming ingestion of flights_latest

Raw data stored in Parquet with minimal transformations

Checkpointing enabled for fault tolerance

🥈 Silver Layer – Cleaned & Integrated Data

Schema standardization

Null handling and imputation

Feature engineering (delay minutes, cancellation flags)

Batch and streaming data union

Star schema creation:

Fact: fact_flights

Dimensions: dim_airlines, dim_airports

🥇 Gold Layer – Business Aggregations

Airline-level cancellation metrics

Airport-level delay analysis

Monthly trend analysis

Optimized for Power BI and analytics consumption

⚙️ Technologies Used

Apache Spark (PySpark)

Spark Structured Streaming

Azure Data Lake Storage Gen2

Parquet + Snappy Compression

Databricks Jobs (Scheduling)

🚀 Key Features

✔ Batch + Streaming integration
✔ Fault-tolerant streaming with checkpoints
✔ Scalable Medallion Architecture
✔ Star schema modeling
✔ Analytics-ready Gold datasets
✔ No dependency on Azure Data Factory

📊 Sample Analytics Use Cases

Airline cancellation trends

Airport delay performance

Seasonal flight delay patterns

Real-time flight disruption monitoring
