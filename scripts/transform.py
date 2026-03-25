import psycopg2
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def run_transformations():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres_db"),
        user=os.getenv("DB_USER", "admin"),
        password=os.getenv("DB_PASSWORD", "password123"),
        dbname=os.getenv("DB_NAME", "worldbank_etl")
    )
    
    sql_commands = """
    -- 1. Populate dim_indicator
    INSERT INTO warehouse.dim_indicator (indicator_code, indicator_name)
    VALUES 
        ('NY.GDP.PCAP.CD', 'GDP per capita (current US$)'),
        ('FP.CPI.TOTL.ZG', 'Inflation, consumer prices (annual %)'),
        ('SL.UEM.TOTL.ZS', 'Unemployment, total (% of total labor force)')
    ON CONFLICT (indicator_code) DO NOTHING;

    -- 2. Populate dim_country
    INSERT INTO warehouse.dim_country (country_code, country_name, region_id, region_value, income_level_id, income_level_value, capital_city)
    SELECT 
        raw_payload->>'id', raw_payload->>'name', raw_payload->'region'->>'id',
        raw_payload->'region'->>'value', raw_payload->'incomeLevel'->>'id',
        raw_payload->'incomeLevel'->>'value', raw_payload->>'capitalCity'
    FROM staging.raw_worldbank_data WHERE dataset_name = 'country'
    ON CONFLICT (country_code) DO NOTHING;

    -- 3. Audit Missing Countries (Referential Integrity Check) 
    INSERT INTO warehouse.audit_missing_countries (fact_indicator_code, missing_country_code, fact_year, fact_value)
    SELECT 
        raw_payload->'indicator'->>'id',
        raw_payload->>'countryiso3code',
        (raw_payload->>'date')::INT,
        (raw_payload->>'value')::NUMERIC
    FROM staging.raw_worldbank_data raw_data
    WHERE dataset_name IN ('gdp', 'inflation', 'unemployment') 
      AND raw_payload->>'value' IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM warehouse.dim_country dc 
          WHERE dc.country_code = raw_data.raw_payload->>'countryiso3code'
      )
      AND NOT EXISTS (
          SELECT 1 FROM warehouse.audit_missing_countries amc
          WHERE amc.fact_indicator_code = raw_data.raw_payload->'indicator'->>'id'
            AND amc.missing_country_code = raw_data.raw_payload->>'countryiso3code'
            AND amc.fact_year = (raw_data.raw_payload->>'date')::INT
      );

    -- 4. Populate Fact Table (Only valid countries) [cite: 40, 41]
    INSERT INTO warehouse.fact_economic_indicators (country_code, indicator_code, year, value)
    SELECT 
        raw_payload->>'countryiso3code', raw_payload->'indicator'->>'id',
        (raw_payload->>'date')::INT, (raw_payload->>'value')::NUMERIC
    FROM staging.raw_worldbank_data
    WHERE dataset_name IN ('gdp', 'inflation', 'unemployment') 
      AND raw_payload->>'value' IS NOT NULL
      AND raw_payload->>'countryiso3code' IN (SELECT country_code FROM warehouse.dim_country)
    ON CONFLICT (country_code, indicator_code, year) DO NOTHING;

    -- 5. Create/Update Reporting View [cite: 48]
    CREATE OR REPLACE VIEW warehouse.rpt_economic_indicators AS
    SELECT dc.country_name, dc.region_value as region, f.year,
           MAX(CASE WHEN f.indicator_code = 'NY.GDP.PCAP.CD' THEN f.value END) as gdp_per_capita,
           MAX(CASE WHEN f.indicator_code = 'FP.CPI.TOTL.ZG' THEN f.value END) as inflation_rate,
           MAX(CASE WHEN f.indicator_code = 'SL.UEM.TOTL.ZS' THEN f.value END) as unemployment_rate
    FROM warehouse.fact_economic_indicators f
    JOIN warehouse.dim_country dc ON f.country_code = dc.country_code
    GROUP BY dc.country_name, dc.region_value, f.year;
    """
    
    try:
        with conn.cursor() as cur:
            logging.info("Starting SQL Transformations with Audit checks...")
            cur.execute(sql_commands)
        conn.commit()
        logging.info("✅ Transformations complete! Warehouse is ready.")
    except Exception as e:
        logging.error(f"Transformation failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    run_transformations()