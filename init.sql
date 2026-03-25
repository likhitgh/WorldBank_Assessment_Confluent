-- 1. Create Schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;

-- 2. Staging Table (Raw JSONB)
CREATE TABLE staging.raw_worldbank_data (
    id SERIAL PRIMARY KEY,
    dataset_name VARCHAR(50),      
    raw_payload JSONB,             
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Data Warehouse: Dimension Tables [cite: 40]
CREATE TABLE warehouse.dim_country (
    country_code VARCHAR(10) PRIMARY KEY,
    country_name VARCHAR(255),
    region_id VARCHAR(50),
    region_value VARCHAR(255),
    income_level_id VARCHAR(50),
    income_level_value VARCHAR(255),
    capital_city VARCHAR(255)
);

CREATE TABLE warehouse.dim_indicator (
    indicator_code VARCHAR(50) PRIMARY KEY,
    indicator_name VARCHAR(255)
);

-- 4. Audit Table for missing foreign keys [cite: 44, 46]
CREATE TABLE warehouse.audit_missing_countries (
    id SERIAL PRIMARY KEY,
    fact_indicator_code VARCHAR(50),
    missing_country_code VARCHAR(10),
    fact_year INT,
    fact_value NUMERIC,
    logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. Data Warehouse: Fact Table [cite: 40, 41]
CREATE TABLE warehouse.fact_economic_indicators (
    id SERIAL PRIMARY KEY,
    country_code VARCHAR(10),  
    indicator_code VARCHAR(50) REFERENCES warehouse.dim_indicator(indicator_code),
    year INT,
    value NUMERIC,
    -- We don't enforce a strict FK on country_code here to allow us to handle 
    -- missing countries gracefully as required by the assessment.
    CONSTRAINT unique_fact UNIQUE (country_code, indicator_code, year)
);