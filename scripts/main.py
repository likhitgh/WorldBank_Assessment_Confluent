import extract
import transform
import logging

if __name__ == "__main__":
    logging.info("--- PHASE 1: EXTRACTION ---")
    extract.run_pipeline()
    
    logging.info("--- PHASE 2: TRANSFORMATION ---")
    transform.run_transformations()
    
    logging.info("--- FULL ETL PIPELINE COMPLETE ---")