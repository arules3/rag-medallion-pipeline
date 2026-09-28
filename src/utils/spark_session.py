import logging
from pyspark.sql import SparkSession

def get_spark_session(app_name : str = "MedallionPipeline") -> SparkSession:
    """
    Initializes and returns a configured SparkSession.
    """
    try:
        spark = SparkSession.builder \
                .appName(app_name) \
                .config("spark.sql.session.timeZone" , "UTC") \
                .config("spark.driver.memory", "4g") \
                .getOrCreate()
            
        logging.info(f"SparkSession '{app_name}' initialized successfully")
        return spark
    
    except Exception as e:
        logging.error(f"failed to initialize spark session: {str(e)} ")


             