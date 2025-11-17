from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from validation_keep.functions import *

def output_keep_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        lit("KEEP").alias("rule_type"), 
        lit(None).cast(StringType()).alias("sub_rule_type"), 
        col("bronze_exists"), 
        col("silver_exists"), 
        col("column_name"), 
        col("bronze_distinct_count"), 
        col("silver_distinct_count"), 
        lit(Config.bronze_path).alias("bronze_table_name"), 
        lit(Config.silver_path).alias("silver_table_name"), 
        when(
            (
              (col("bronze_exists") & col("silver_exists"))
              & (col("bronze_distinct_count") == col("silver_distinct_count"))
            ), 
            lit(True)
          )\
          .otherwise(lit(False))\
          .alias("rule_status"), 
        current_timestamp().alias("validation_timestamp")
    )
