from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from validation_mask.config.ConfigStore import *
from validation_mask.functions import *
from prophecy.utils import *

def pipeline(spark: SparkSession) -> None:
    pass

def main():
    spark = SparkSession.builder.enableHiveSupport().appName("validation_mask").getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/validation_mask")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/validation_mask", config = Config)(pipeline)

if __name__ == "__main__":
    main()
