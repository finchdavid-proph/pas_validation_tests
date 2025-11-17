from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from validation_map_2.config.ConfigStore import *
from validation_map_2.functions import *

def postal_code_validation_status_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        when((sum(when(col("BIRTH_DATETIME").isNull(), lit(1)).otherwise(lit(0))) == count(lit(1))), lit("FAILED - NULL"))\
          .when(
            (
              sum(when((col("BIRTH_DATETIME").isNotNull() & (col("BIRTH_DATETIME") < (current_date() - expr("INTERVAL '89' YEAR")))), lit(1)).otherwise(lit(0)))
              == lit(0)
            ), 
            lit("PASSED")
          )\
          .otherwise(lit("FAILED"))\
          .alias("rule_status"), 
        lit("sample_data_clinical_patient").alias("source_table"), 
        lit("birth_datetime").alias("column"), 
        lit("mapping").alias("rule_type")
    )
