from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from validation_keep.functions import *

def compare_distinct_counts_1(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.column_name") == col("in1.column_name")), "left_outer")\
        .select(col("in0.column_name").alias("column_name"), col("in0.bronze_exists").alias("bronze_exists"), col("in0.silver_exists").alias("silver_exists"), col("in0.bronze_distinct_count").alias("bronze_distinct_count"), col("in1.silver_distinct_count").alias("silver_distinct_count"))
