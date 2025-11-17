from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from validation_sha256.config.ConfigStore import *
from validation_sha256.functions import *
from prophecy.utils import *
from validation_sha256.graph import *

def pipeline(spark: SparkSession) -> None:
    df_mapping_table = mapping_table(spark)
    df_filter_sha256_true = filter_sha256_true(spark, df_mapping_table)
    iterator_validation_sha256(Config.iterator_validation_sha256).apply(spark, df_filter_sha256_true)

def main():
    spark = SparkSession.builder.enableHiveSupport().appName("validation_sha256").getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/validation_sha256")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/validation_sha256", config = Config)(pipeline)

if __name__ == "__main__":
    main()
