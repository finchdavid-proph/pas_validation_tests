from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from validation_suppress.functions import *

def compare_suppress_1(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.bronze_column_name") == col("in1.silver_col_name")), "left_outer")\
        .select(col("in0.bronze_column_name").alias("column_name"), lit(True).alias("bronze_exists"), col("in1.silver_col_name").isNotNull().alias("silver_exists"), lit("SUPPRESS").alias("rule_type"), lit(None).cast(StringType()).alias("sub_rule_type"), lit(Config.bronze_path).alias("bronze_table_name"), lit(Config.silver_path).alias("silver_table_name"), (col("bronze_exists") & (~ col("silver_exists"))).alias("rule_status"), current_timestamp().alias("validation_timestamp"))
