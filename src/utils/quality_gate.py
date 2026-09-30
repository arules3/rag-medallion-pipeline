# src/utils/quality_gate.py
import great_expectations as gx
import great_expectations.expectations as gxe
from pyspark.sql import DataFrame

class QualityGate:
    @staticmethod
    def validate(df: DataFrame, suite_name: str, expectations: list) -> bool:
        context = gx.get_context(mode="ephemeral")
        ds = context.data_sources.add_spark(name="spark_source")
        asset = ds.add_dataframe_asset(name="df_asset")
        batch = asset.add_batch_definition_whole_dataframe("batch").get_batch({"dataframe": df})
        
        suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
        for exp in expectations:
            suite.add_expectation(exp)
            
        result = batch.validate(suite)
        return result.success