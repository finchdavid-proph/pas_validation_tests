from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from validation_keep.functions import *

def join_bronze_silver_1(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.bronze_col_name") == col("in1.silver_col_name")), "outer")\
        .select(coalesce(col("in0.bronze_col_name"), col("in1.silver_col_name")).alias("column_name"), col("in0.bronze_col_name").isNotNull().alias("bronze_exists"), col("in1.silver_col_name").isNotNull().alias("silver_exists"), lit("KEEP").alias("rule_type"), lit(None).cast(StringType()).alias("sub_rule_type"))
