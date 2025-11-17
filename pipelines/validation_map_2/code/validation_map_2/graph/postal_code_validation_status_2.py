from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from validation_map_2.config.ConfigStore import *
from validation_map_2.functions import *

def postal_code_validation_status_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        when(
            (sum(when(col("STD_ADMINISTRATIVE_SEX_CODE").isNull(), lit(1)).otherwise(lit(0))) == count(lit(1))), 
            lit("FAILED - NULL")
          )\
          .when(
            (
              sum(when((col("STD_ADMINISTRATIVE_SEX_CODE").isNotNull() & (~ col("STD_ADMINISTRATIVE_SEX_CODE").isin(lit("F"), lit("M"), lit("U")))), lit(1)).otherwise(lit(0)))
              == lit(0)
            ), 
            lit("PASSED")
          )\
          .otherwise(lit("FAILED"))\
          .alias("rule_status"), 
        lit("sample_data_clinical_patient").alias("source_table"), 
        lit("std_administrative_sex_code").alias("column"), 
        lit("mapping").alias("rule_type")
    )
