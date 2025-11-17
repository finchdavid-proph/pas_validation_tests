from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from validation_pk.config.ConfigStore import *
from validation_pk.functions import *
from prophecy.utils import *
from validation_pk.graph import *

def pipeline(spark: SparkSession) -> None:
    df_mapping_table = mapping_table(spark)
    df_filter_keep_true = filter_keep_true(spark, df_mapping_table)
    iterator_validation_keep(Config.iterator_validation_keep).apply(spark, df_filter_keep_true)

def main():
    spark = SparkSession.builder.enableHiveSupport().appName("validation_pk").getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/validation_pk")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/validation_pk", config = Config)(pipeline)

if __name__ == "__main__":
    main()
