from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from validation_pk.functions import *

def source_bronze_pk_1(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"`{Config.bronze_catalog}`.`{Config.bronze_schema}`.`{Config.bronze_table}`")
