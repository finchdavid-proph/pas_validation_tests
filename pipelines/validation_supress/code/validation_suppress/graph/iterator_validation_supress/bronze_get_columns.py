from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from validation_suppress.functions import *

def bronze_get_columns(spark: SparkSession, in0: DataFrame) -> (DataFrame):

    try:
        registerUDFs(spark)
    except NameError:
        print("registerUDFs not working")

    in0.createOrReplaceTempView("in0")
    df1 = spark.sql(f"SHOW COLUMNS IN {Config.bronze_path}")

    return df1
