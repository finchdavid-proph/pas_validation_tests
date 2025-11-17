from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from validation_sha256.config.ConfigStore import *
from validation_sha256.functions import *

def filter_sha256_true(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("sha256") == lit(True)))
