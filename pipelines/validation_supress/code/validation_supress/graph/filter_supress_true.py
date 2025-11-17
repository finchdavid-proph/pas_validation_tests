from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from validation_supress.config.ConfigStore import *
from validation_supress.functions import *

def filter_supress_true(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("supress") == lit(True)))
