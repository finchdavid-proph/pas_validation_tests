from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from validation_map.config.ConfigStore import *
from validation_map.functions import *
from prophecy.utils import *
from validation_map.graph import *

def pipeline(spark: SparkSession) -> None:
    df_val_map_2 = val_map_2(spark)
    TableIterator_1(Config.TableIterator_1).apply(spark, df_val_map_2)

def main():
    spark = SparkSession.builder.enableHiveSupport().appName("validation_map").getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/validation_map")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/validation_map", config = Config)(pipeline)

if __name__ == "__main__":
    main()
