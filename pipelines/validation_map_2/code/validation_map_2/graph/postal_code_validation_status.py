from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from validation_map_2.config.ConfigStore import *
from validation_map_2.functions import *

def postal_code_validation_status(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        when((sum(when(col("POSTAL_CODE").isNull(), lit(1)).otherwise(lit(0))) == count(lit(1))), lit("FAILED - NULL"))\
          .when(
            (
              sum(when(col("POSTAL_CODE").isin(lit("036"), lit("059"), lit("090"), lit("091"), lit("092"), lit("093"), lit("094"), lit("095"), lit("096"), lit("097"), lit("098"), lit("099"), lit("102"), lit("202"), lit("203"), lit("204"), lit("205"), lit("340"), lit("369"), lit("556"), lit("692"), lit("753"), lit("772"), lit("821"), lit("823"), lit("878"), lit("879"), lit("884"), lit("893"), lit("962"), lit("963"), lit("964"), lit("965"), lit("966"), lit("987")), lit(1)).otherwise(lit(0)))
              == lit(0)
            ), 
            lit("PASSED")
          )\
          .otherwise(lit("FAILED"))\
          .alias("rule_status"), 
        lit("sample_data_clinical_patient").alias("source_table"), 
        lit("postal_code").alias("column"), 
        lit("mapping").alias("rule_type")
    )
