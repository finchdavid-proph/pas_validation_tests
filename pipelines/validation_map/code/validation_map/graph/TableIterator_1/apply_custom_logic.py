from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from validation_map.functions import *

def apply_custom_logic(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(expr(Config.logic).alias("rule_status"))
