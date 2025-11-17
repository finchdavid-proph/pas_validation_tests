from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from validation_pk.functions import *

def count_records(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        when((count(lit(f"DISTINCT {Config.pk}")) == count(lit(1))), lit("true"))\
          .otherwise(lit("false"))\
          .alias("rule_status")
    )
