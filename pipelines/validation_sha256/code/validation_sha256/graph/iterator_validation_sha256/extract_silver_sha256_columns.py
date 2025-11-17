from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from validation_sha256.functions import *

def extract_silver_sha256_columns(spark: SparkSession, in0: DataFrame) -> (DataFrame):

    try:
        registerUDFs(spark)
    except NameError:
        print("registerUDFs not working")

    in0.createOrReplaceTempView("in0")
    df1 = spark.sql(f"SELECT explode(from_json(\"{Config.silver_sha256_columns}\", 'array<string>')) AS column_name")

    return df1
