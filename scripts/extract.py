import requests
import psycopg2
from psycopg2.extras import Json
import os
import logging
from typing import Iterator, Dict, Any, Optional

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

class WorldBankAPIClient:
    """A client to safely extract data from the World Bank API with pagination."""
    BASE_URL = "https://api.worldbank.org/v2"

    # FIX: Lower per_page to prevent API server overload
    def __init__(self, per_page: int = 500):
        self.per_page = per_page

    def fetch_paginated_data(self, endpoint: str, date_range: Optional[str] = None) -> Iterator[list[Dict[Any, Any]]]:
        url = f"{self.BASE_URL}/{endpoint}"
        page = 1
        total_pages = 1

        while page <= total_pages:
            params = {"format": "json", "per_page": self.per_page, "page": page}
            if date_range:
                params["date"] = date_range

            logging.info(f"Fetching {endpoint} - Page {page}/{total_pages}...")
            
            # FIX : Increased timeout to 60 seconds to give the API more time to respond
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            if len(data) < 2 or not data[1]:
                break
                
            metadata, records = data[0], data[1]
            total_pages = metadata.get("pages", 1)
            
            yield records
            page += 1

def get_db_connection():
    """Connects to Postgres using environment variables with local defaults."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres_db"), 
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("DB_USER", "admin"),
        password=os.getenv("DB_PASSWORD", "password123"),
        dbname=os.getenv("DB_NAME", "worldbank_etl")
    )

def load_to_staging(conn, dataset_name: str, records: list):
    """Inserts raw JSON records into the Postgres staging table."""
    insert_query = """
        INSERT INTO staging.raw_worldbank_data (dataset_name, raw_payload)
        VALUES (%s, %s);
    """
    # Wrap each record in psycopg2's Json() helper
    data_to_insert = [(dataset_name, Json(record)) for record in records]
    
    with conn.cursor() as cur:
        cur.executemany(insert_query, data_to_insert)
    conn.commit()
    logging.info(f"✅ Inserted {len(records)} records into staging for '{dataset_name}'.")

def run_pipeline():
    logging.info("Starting World Bank Data Pipeline...")
    client = WorldBankAPIClient()
    date_range = "2020:2023"
    
    # Establish database connection
    conn = get_db_connection()
    
    try:
        # 1. Extract and Load Countries
        for batch in client.fetch_paginated_data("country"):
            load_to_staging(conn, "country", batch)

        # 2. Extract and Load GDP
        for batch in client.fetch_paginated_data("country/all/indicator/NY.GDP.PCAP.CD", date_range):
            load_to_staging(conn, "gdp", batch)

        # 3. Extract and Load Inflation
        for batch in client.fetch_paginated_data("country/all/indicator/FP.CPI.TOTL.ZG", date_range):
            load_to_staging(conn, "inflation", batch)

        # 4. Extract and Load Unemployment
        for batch in client.fetch_paginated_data("country/all/indicator/SL.UEM.TOTL.ZS", date_range):
            load_to_staging(conn, "unemployment", batch)
            
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        # FIX 3: Raise the exception so Airflow properly marks the task as failed
        raise  
    finally:
        conn.close()
        logging.info("Database connection closed. Pipeline complete!")

if __name__ == "__main__":
    run_pipeline()