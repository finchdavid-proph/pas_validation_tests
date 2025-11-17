from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from validation_supress.config.ConfigStore import *
from validation_supress.functions import *
from prophecy.utils import *
from validation_supress.graph import *

def pipeline(spark: SparkSession) -> None:
    df_mapping_table = mapping_table(spark)
    df_filter_supress_true = filter_supress_true(spark, df_mapping_table)
    iterator_validation_supress(Config.iterator_validation_supress).apply(spark, df_filter_supress_true)

def main():
    spark = SparkSession.builder.enableHiveSupport().appName("validation_supress").getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/validation_supress")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/validation_supress", config = Config)(pipeline)

if __name__ == "__main__":
    main()
