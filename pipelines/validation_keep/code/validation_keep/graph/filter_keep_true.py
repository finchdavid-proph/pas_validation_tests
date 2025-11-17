from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from validation_keep.config.ConfigStore import *
from validation_keep.functions import *

def filter_keep_true(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("keep") == lit(True)))
