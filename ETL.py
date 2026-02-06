# ==============================
# STEP 1: IMPORT LIBRARIES
# ==============================
import pandas as pd
from datetime import datetime
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os
from dotenv import load_dotenv
 
load_dotenv()
 
# ==============================
# STEP 2: READ SOURCE FILES
# ==============================
csv_df = pd.read_csv("source_data.csv")
excel_df = pd.read_excel("source_data.xlsx")
# ==============================
# STEP 3: ADD SOURCE COLUMN
# ==============================
csv_df["SOURCE"] = "CSV"
excel_df["SOURCE"] = "EXCEL"
# ==============================
# STEP 4: CREATE RAW LAYER
# ==============================
raw_df = pd.concat([csv_df, excel_df], ignore_index=True)
def clean_gender(value):
    if pd.isna(value):
        return "O"
    value = value.strip().lower()
    if value in ["male", "m"]:
        return "M"
    elif value in ["female", "f"]:
        return "F"
    else:
        return "O"

raw_df["GENDER"] = raw_df["GENDER"].apply(clean_gender)
raw_df["DOB"] = pd.to_datetime(raw_df["DOB"], errors="coerce") \
                  .dt.strftime("%d-%m-%Y")
raw_df["LOAD_TIMESTAMP"] = datetime.now()
final_df = pd.merge(
    csv_df,
    excel_df,
    on="USER_ID",
    how="inner",
    suffixes=("_CSV", "_EXCEL")
)
final_df["DOB"] = pd.to_datetime(final_df["DOB_CSV"], errors="coerce")

today = pd.Timestamp.today()
final_df["AGE"] = (today - final_df["DOB"]).dt.days // 365
final_df = final_df[final_df["AGE"] > 18]
final_df["LOAD_TIMESTAMP"] = datetime.now()
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)
raw_df = raw_df.reset_index(drop=True)
final_df = final_df.reset_index(drop=True)
 
    # Ensure uppercase column names
raw_df.columns = raw_df.columns.str.upper()
final_df.columns = final_df.columns.str.upper()
# Load RAW layer
write_pandas(conn, raw_df, "RAW_USER_DATA",auto_create_table=True)
# Load FINAL layer
write_pandas(conn, final_df, "FINAL_USER_DATA",auto_create_table=True)
#conn.close()
print("ETL Pipeline Completed Successfully")

print("RAW rows:", len(raw_df))
print("FINAL rows:", len(final_df))