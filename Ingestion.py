import os
import time
import requests
import pandas as pd
from sqlalchemy import create_engine
import sys

#PostgreSQL connection URL from Neon Console
os. environ ["PG_URL"] = (
    "postgresql+psycopg2://neondb_owner:npg_2wdZpRaF0LmG@"
    "ep-patient-dawn-advbhv1g-pooler.c-2.us-east-1.aws.neon.tech/"
    "neondb?sslmode=require"
)

#CDC COVID-19 dataset API endpoint
API_URL = "https://data.cdc.gov/resource/n8mc-b4w4.json"

#columns to retain from the API response
KEEP_COLS = [
    "case_month", "cdc_case_earliest_dt", "res_state",
    "age_group", "sex", "race", "ethnicity",
    "death_yn", "hosp_yn", "icu_yn", "medcond_yn"]

#function to create a database engine
def get_engine():
    """Create a PostgreSQL SQLAlchemy engine from PG_URL."""
    pg_url = os.environ.get("PG_URL")
    if not pg_url:
        #exit if PG_URL is not set
        raise SystemExit("please set PG_URL in Colab first.")
    #return the database engine
    return create_engine(pg_url, pool_pre_ping=True)

#function to fetch data from the API
def fetch_page(limit, offset, where=None):
    #set up parameters for the API request
    parameters = {"$limit": limit, "$offset": offset}
    if where:
        parameters["$where"] = where

    try:
        r = requests.get(API_URL, params=parameters, timeout=60) #Hit api
        r.raise_for_status()  #raise an exception for bad status codes
        return r.json()
    except requests.exceptions.HTTPError as er:
        if er.response.status_code == 429:
            print("wait 2 seconds before retry bcoz rate limit hit")
            time.sleep(2)  #wait for 2 seconds before retrying
            r = requests.get(API_URL, params=parameters, timeout=60) #try again
            r.raise_for_status() #raise an exception for bad status codes on retry
            return r.json()
        else:
            raise er #raise other HTTP errors
    except requests.exceptions.RequestException as er:
        print(f"Request error: {er}")
        return None


#function to convert API response to DataFrame
def transform_rows_to_df(rows):
    """converting API JSON response to a cleaned pandas DataFrame."""
    if not rows:
        return pd.DataFrame(columns=KEEP_COLS) # Return empty DataFrame if no rows

    df = pd.DataFrame.from_records(rows)

    # Ensure all target columns exist, add if missing
    for c in KEEP_COLS:  #checks for our specified columns
        if c not in df.columns:
            df[c] = None

    df = df[KEEP_COLS].copy() #select and copy specified columns to data frame
    return df

def clean_df(df: pd.DataFrame) -> pd.DataFrame:   #creates a function which expects data frame as input and returns also a dataframe
    df = df.copy()
    #remove duplicate rows
    df = df.drop_duplicates()

    #convert date strings to datetime objects as part of cleansing the data
    if "cdc_case_earliest_dt" in df.columns:
        df["cdc_case_earliest_dt"] = pd.to_datetime(df["cdc_case_earliest_dt"], errors="coerce")

    #clean and standardize text columns
    if "res_state" in df.columns:
        df["res_state"] = df["res_state"].astype(str).str.strip().str.upper()

    for c in ["case_month", "age_group", "sex", "race", "ethnicity",
              "death_yn", "hosp_yn", "icu_yn", "medcond_yn"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    #handle nulls: fill blank or "None"/"nan" with "Unknown" for categorical columns
    categorical_cols = ["age_group","sex","race","ethnicity","death_yn","hosp_yn","icu_yn","medcond_yn"]
    for c in categorical_cols:
        if c in df.columns:
            df[c] = df[c].replace({"None": None, "nan": None, "": None})
            df[c] = df[c].fillna("Unknown")

    return df

#function to write DataFrame to our PostgreSQL
def load_df_to_postgres(engine, df):
    #append data to the specified table in PostgreSQL
    df.to_sql(
        name="covid_case_surveillance",
        con=engine,
        if_exists="append",
        index=False,
        chunksize=10000,
        method="multi"
    )

#function to write RAW DataFrame to our PostgreSQL (landing table)
def load_df_to_postgres_raw(engine, df):
    # append into the RAW landing table
    df.to_sql(
        name="covid_case_surveillance_raw",
        con=engine,
        if_exists="append",
        index=False,
        chunksize=10000,
        method="multi"
    )    

#main function to orchestrate the process
def main():
    #defining default values for page size and max records, in our case anything more than 100k so I choose 150k
    page_size = 50000
    max_records = 150000
    where = None

    #get the database engine
    eng = get_engine()
    total_inserted = 0
    offset = 0
    total_fetched = 0

    while total_fetched < max_records:
        #determine how many records to fetch in the current page
        need = min(page_size, max_records - total_fetched)
        #fetching data from the API
        print(f"\nfetching {need} rows starting at offset {offset}")
        rows = fetch_page(limit=need, offset=offset, where=where)
        if not rows:
            print("no enough rows here so exiting.")
            break
        fetched_count = len(rows)
        total_fetched +=fetched_count
        
        #converting fetched rows to a DataFrame (RAW page)
        df = transform_rows_to_df(rows)

        #tag this batch so we can read back exactly what we just inserted
        batch_ts = pd.Timestamp.utcnow()
        df["_ingested_at"] = batch_ts

        #write RAW to Postgres (landing table)
        if not df.empty:
            load_df_to_postgres_raw(eng, df)
        else:
            print("Raw DataFrame is empty.")

        #read back the RAW rows we just inserted
        raw_sql = "SELECT * FROM covid_case_surveillance_raw WHERE _ingested_at = %(ts)s"
        df_raw_from_pg = pd.read_sql_query(raw_sql, eng, params={"ts": batch_ts})

        #drop the helper column before cleaning
        if "_ingested_at" in df_raw_from_pg.columns:
            df_raw_from_pg = df_raw_from_pg.drop(columns=["_ingested_at"])

        #clean in-memory
        df_clean = clean_df(df_raw_from_pg)

        print(f"fetched_count={fetched_count}, cleaned_count={len(df_clean)}")

        #write clean to table
        if not df_clean.empty:
            load_df_to_postgres(eng, df_clean)
            total_inserted += len(df_clean)
            print(f"inserted : {total_inserted} for now")
        else:
            print("DataFrame is empty after cleaning.")

            
        offset += fetched_count
        time.sleep(0.7) #adding delay
        

        #checking if the fetched page was shorter than requested
        if fetched_count < need:
            print("not enough, short dataset-Thanks.")
            break

    #printing total records inserted
    print(f"\nprocess finished.Total inserted:{total_inserted}")

#execute the main function when the script is run
if __name__ == "__main__":
    main()
