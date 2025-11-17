from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from validation_suppress.functions import *

def reformat_bronze_path_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("bronze_table_name"), 
        col("silver_table_name"), 
        col("column_name"), 
        col("rule_type"), 
        col("sub_rule_type"), 
        col("rule_status"), 
        col("validation_timestamp")
    )
