# ETL Project – CSV & Excel to Snowflake

## 📌 Project Description
This project demonstrates an **end-to-end ETL (Extract, Transform, Load) pipeline** built using **Python, Pandas, and Snowflake**.

The pipeline:
- Reads data from **CSV and Excel** source files
- Performs **data cleaning and transformations**
- Loads data into **Snowflake** using `write_pandas`
- Follows **best practices** such as environment variables and `.gitignore`

This project is suitable for **data engineering training and real-world ETL understanding**.

PYTHON_TRAINING/
│
├── ETL.py # Main ETL pipeline script
├── source_data.csv # CSV source file
├── source_data.xlsx # Excel source file
├── .env # Environment variables (ignored by Git)
├── .gitignore # Git ignore rules
└── README.md # Project documentation



### Extract
- Reads data from:
  - `source_data.csv`
  - `source_data.xlsx`

### Transform
* Adds SOURCE column (CSV / EXCEL)
* Cleans GENDER values (M, F, O)
* Converts DOB to date format
* Adds LOAD_TIMESTAMP
Creates two layers:
1. RAW layer – combined data
2. FINAL layer – merged data with age calculation

Business Rules:
Age calculated from DOB
Only users with AGE > 18 included in FINAL layer
Column names converted to UPPERCASE

### Load
Loads data into Snowflake tables: RAW_USER_DATA, FINAL_USER_DATA
Uses write_pandas with auto table creation
write_pandas(conn, raw_df, "RAW_USER_DATA", auto_create_table=True)
write_pandas(conn, final_df, "FINAL_USER_DATA", auto_create_table=True)

## Snowflake Tables
1. RAW_USER_DATA
* Contains all records from CSV and Excel
* Minimal transformation
* Used for audit and traceability

2. FINAL_USER_DATA
* Contains only matched users
* Includes calculated AGE
* Filters users above 18 years

## Environment Configuration
Create a .env file in the root directory:

SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema

## Snowflake Table Creation
# RAW Layer Table
CREATE OR REPLACE TABLE RAW_USER_DATA ( USER_ID STRING, FULL_NAME STRING, GENDER STRING, DOB DATE, EMAIL STRING, COUNTRY STRING, ACCOUNT_CREATED TIMESTAMP, SOURCE_SYSTEM STRING, LOAD_TIMESTAMP TIMESTAMP );

# FINAL Layer Table
CREATE OR REPLACE TABLE FINAL_USER_DATA ( USER_ID STRING, FULL_NAME STRING, GENDER STRING, DOB DATE, AGE INTEGER );

## Run the Pipeline
python etl_pipeline.py


### Author
### Pochamreddy Pavani
Data Engineering Project


