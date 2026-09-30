import logging
import sys 
from pyspark.sql.functions import current_timestamp , lit , col, trim
import great_expectations as gx
import great_expectations.expectations as gxe


import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.utils.spark_session import get_spark_session
from src.utils.logger import get_logger
from src.utils.quality_gate import QualityGate

logger = get_logger(__name__)



def run_bronze_ingestion():
    spark = get_spark_session("BronzeIngestion")
    raw_path = "data/raw/complaints.csv"
    bronze_path = "data/bronze/complaints.parquet"
    
    logger.info(f"reading raw data from {raw_path}")
    df = spark.read.csv(raw_path , header=True , inferSchema=True , multiLine=True, quote='"', escape='"', ignoreLeadingWhiteSpace=True , ignoreTrailingWhiteSpace=True)
    df.printSchema()
    
    # 1. DYNAMIC SCHEMA NORMALIZATION
    # Strip all leading/trailing whitespace, replace spaces with underscores, and lowercase
    # This protects the pipeline from upstream CSV formatting bugs.
    normalized_columns = [col.strip().replace(" ", "_").replace("?", "").lower() for col in df.columns]
    df = df.toDF(*normalized_columns)
    
    # 2. GREAT EXPECTATIONS QUALITY GATE
    logger.info("Running Bronze Quality Gate...")
    passed = QualityGate.validate(
        df = df,
        suite_name="bronze_contract",
        expectations=[
        gxe.ExpectColumnToExist(column="complaint_id"),
        gxe.ExpectColumnToExist(column="issue"),
        gxe.ExpectColumnToExist(column="company"),
        gxe.ExpectColumnValuesToNotBeNull(column="complaint_id"),
        ])


    if not passed:
        logger.error("Data contract failed validation against Great Expectations suite.")
        raise ValueError("Raw data violates the ingestion data contract.")

    logger.info("Quality Gate Passed.")

    # 3. ADD AUDIT METADATA
    df_bronze = df.withColumn("ingested_at", current_timestamp()) \
                  .withColumn("source_system", lit("CFPB_API"))
    
        

    # 4. WRITE TO BRONZE
    logger.info(f"Writing validated data to Bronze layer at {bronze_path}")
    df_bronze.write.mode("overwrite").parquet(bronze_path)
    
    logger.info("Bronze ingestion complete.")
    spark.stop()
    
if __name__ == "__main__" :
    run_bronze_ingestion()
    