import logging
import sys 
from pyspark.sql.functions import current_timestamp , lit , col, trim
from great_expectations.dataset import SparkDFDataset

import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.utils.spark_session import get_spark_session
from src.utils.logger import get_logger

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
    gx_df = SparkDFDataset(df)
    
    # Contract Check 1: Primary Key must exist
    res_id = gx_df.expect_column_to_exist("complaint_id")
    
    # Contract Check 2: Core fields for our RAG system must exist
    res_issue = gx_df.expect_column_to_exist("issue")
    res_company = gx_df.expect_column_to_exist("company")
    
    # Contract Check 3: Primary key must not be null
    res_not_null = gx_df.expect_column_values_to_not_be_null("complaint_id")

    # Evaluate Gate
    if not (res_id["success"] and res_issue["success"] and res_company["success"] and res_not_null["success"]):
        logging.error(f"Schema normalization failed to satisfy contract. Found columns: {df.columns}")
        raise ValueError("Raw data violates the ingestion data contract.")

    logger.info("Quality Gate Passed.")

    # 3. ADD AUDIT METADATA
    df_bronze = df.withColumn("ingested_at", current_timestamp()) \
                  .withColumn("source_system", lit("CFPB_API"))
    
    
    #df_bronze= df.withColumn("complaint_id" , trim(col("complaint_id")))
        

    # 4. WRITE TO BRONZE
    logger.info(f"Writing validated data to Bronze layer at {bronze_path}")
    df_bronze.write.mode("overwrite").parquet(bronze_path)
    
    logger.info("Bronze ingestion complete.")
    spark.stop()
    
if __name__ == "__main__" :
    run_bronze_ingestion()
    