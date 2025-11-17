from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from validation_supress.functions import *

def rename_column_silver(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("col_name").alias("silver_col_name"))
