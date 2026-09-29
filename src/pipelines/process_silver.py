import os 
import sys

from pyspark.sql.functions import col ,concat_ws , trim , lit , when , current_timestamp
from great_expectations.dataset import SparkDFDataset


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__) , "../..")))
from src.utils.spark_session import get_spark_session
from src.utils.logger import get_logger

logger = get_logger(__name__)




def process_silver_data():
    spark = get_spark_session("SilverProcessing")
    bronze_path = "data/bronze/complaints.parquet"
    silver_path = "data/silver/complaints.parquet"
    
    logger.info("Reading the bronze layer data...")
    
    
    # 1. CLEANING & STANDARDIZATION
  
    
    df = spark.read.parquet(bronze_path)
    
    logger.info("Applying cleaning transformations...")
    df_clean = df.withColumn("product", trim(col("product"))) \
               .withColumn("issue" , trim(col("issue"))) \
               .filter(col("complaint_id").isNotNull()) \
               .filter(col("issue").isNotNull())
               
               
    # 2. FEATURE ENGINEERING FOR RAG (Synthetic Narrative Construction)
    # Because raw narratives were missing in our API sample, we stitch together
    # a structured context string. This allows our RAG system to semantically search complaints.
    logger.info("Constructing synthetic text documents for vectorization...")
    df_silver = df_clean.withColumn(
        "rag_text",
        concat_ws(
            " | ",
            lit("Product: ") + col("product"),
            lit("Sub-product: ") + when(col("sub-product").isNull(), lit("N/A")).otherwise(col("sub-product")),
            lit("Issue: ") + col("issue"),
            lit("Company Response: ") + when(col("company_response_to_consumer").isNull(), lit("N/A")).otherwise(col("company_response_to_consumer"))
        )
    )
    
    
    #3. Silver Quality Gate
    
    logger.info("Running Silver Quality Gate...")
    gx_df = SparkDFDataset(df_silver)
    
    # Check that our synthesized text column exists and has no nulls
    res_text_exists = gx_df.expect_column_to_exist("rag_text")
    res_text_not_null = gx_df.expect_column_values_to_not_be_null("rag_text")
    
    if not (res_text_exists["success"] and res_text_not_null["success"]):
        logger.error("Silver Data Quality Gate Failed! RAG text field is invalid.")
        raise ValueError("Silver layer data contract violated.")
    
    logger.info("Silver Quality Gate Passed.")
    
    # 4. ADD AUDIT METADATA
    logger.info(f"Adding metata to {silver_path}")
    df_silver = df_silver.withColumn("processed_at", current_timestamp())
    
    # 5. WRITE TO SILVER
    logger.info(f"Writing cleaned data to Silver layer at {silver_path}")
    df_silver.write.mode("overwrite").parquet(silver_path)
    
    logger.info(f"Silver processing complete. Total records processed: {df_silver.count()}")
    
    
    spark.stop()
    
    
    
if __name__ == "__main__":
    process_silver_data()
   
