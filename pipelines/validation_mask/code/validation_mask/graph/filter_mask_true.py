from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from validation_mask.config.ConfigStore import *
from validation_mask.functions import *

def filter_mask_true(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("mask") == lit(True)))
