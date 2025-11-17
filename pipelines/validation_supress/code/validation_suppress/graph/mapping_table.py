from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from validation_suppress.config.ConfigStore import *
from validation_suppress.functions import *

def mapping_table(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"`{Config.mapping_catalog}`.`{Config.mapping_schema}`.`{Config.mapping_table}`")
