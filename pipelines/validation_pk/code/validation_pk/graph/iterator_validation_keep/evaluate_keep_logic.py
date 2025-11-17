from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from validation_pk.functions import *

def evaluate_keep_logic(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        lit("PK").alias("rule_type"), 
        lit(None).cast(StringType()).alias("sub_rule_type"), 
        lit(Config.bronze_path).alias("bronze_table_name"), 
        lit(Config.silver_path).alias("silver_table_name"), 
        lit(Config.pk).alias("column_name"), 
        col("rule_status"), 
        current_timestamp().alias("validation_timestamp")
    )
