from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from validation_mask.functions import *

def source_silver_mask(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"`{Config.silver_catalog}`.`{Config.silver_schema}`.`{Config.silver_table}`")
