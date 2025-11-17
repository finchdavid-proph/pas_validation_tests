from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from validation_map.config.ConfigStore import *
from validation_map.functions import *

def val_map_2(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"`{Config.val_map_cat}`.`{Config.val_map_sch}`.`{Config.val_map_2}`")
