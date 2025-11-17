from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from validation_map_2.config.ConfigStore import *
from validation_map_2.functions import *

def clinical_patient(spark: SparkSession) -> DataFrame:
    return spark.read.table("`dfinch`.`pad`.`sample_data_clinical_patient`")
