from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from validation_sha256.functions import *

def inner_join_suppress_1_1(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.silver_col_name") == col("in1.column_name")), "inner")\
        .select(col("in0.silver_col_name").alias("silver_column_name"))
