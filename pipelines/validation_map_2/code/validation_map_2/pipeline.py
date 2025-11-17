from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from validation_map_2.config.ConfigStore import *
from validation_map_2.functions import *
from prophecy.utils import *
from validation_map_2.graph import *

def pipeline(spark: SparkSession) -> None:
    df_clinical_patient = clinical_patient(spark)
    df_postal_code_validation_status = postal_code_validation_status(spark, df_clinical_patient)
    df_postal_code_validation_status_1 = postal_code_validation_status_1(spark, df_clinical_patient)
    df_postal_code_validation_status_2 = postal_code_validation_status_2(spark, df_clinical_patient)

def main():
    spark = SparkSession.builder.enableHiveSupport().appName("validation_map_2").getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/validation_map_2")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/validation_map_2", config = Config)(pipeline)

if __name__ == "__main__":
    main()
