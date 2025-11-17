from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from validation_sha256.functions import *
from . import *
from .config import *


class iterator_validation_sha256(MetaGemExec):

    def __init__(self, config):
        self.config = config
        super().__init__()

    def execute(self, spark: SparkSession, subgraph_config: SubgraphConfig) -> List[DataFrame]:
        Config.update(subgraph_config)
        df_source_silver_sha256 = source_silver_sha256(spark)
        df_silver_get_columns = silver_get_columns(spark, df_source_silver_sha256)
        df_rename_column_silver = rename_column_silver(spark, df_silver_get_columns)
        df_extract_silver_sha256_columns = extract_silver_sha256_columns(spark, df_source_silver_sha256)
        df_inner_join_suppress_1_1 = inner_join_suppress_1_1(
            spark, 
            df_rename_column_silver, 
            df_extract_silver_sha256_columns
        )
        df_source_bronze_sha256 = source_bronze_sha256(spark)
        df_bronze_get_columns = bronze_get_columns(spark, df_source_bronze_sha256)
        df_rename_column_to_bronze_format = rename_column_to_bronze_format(spark, df_bronze_get_columns)
        df_extract_bronze_sha256_columns = extract_bronze_sha256_columns(spark, df_source_bronze_sha256)
        df_inner_join_suppress_1 = inner_join_suppress_1(
            spark, 
            df_extract_bronze_sha256_columns, 
            df_rename_column_to_bronze_format
        )
        subgraph_config.update(Config)

    def apply(self, spark: SparkSession, in0: DataFrame, ) -> None:
        inDFs = []
        results = []
        conf_to_column = dict(
            [("bronze_catalog", "bronze_catalog"),  ("bronze_schema", "bronze_schema"),  ("bronze_table", "bronze_table"),              ("bronze_path", "bronze_path"),  ("silver_catalog", "silver_catalog"),              ("silver_schema", "silver_schema"),  ("silver_table", "silver_table"),              ("silver_path", "silver_path"),  ("suppress_columns", "suppress_columns"),  ("keep", "keep"),              ("supress", "supress"),  ("map", "map"),  ("sha256", "sha256"),  ("mask", "mask"),              ("bronze_sha256_columns", "bronze_sha256_columns"),              ("silver_sha256_columns", "silver_sha256_columns")]
        )

        if in0.count() > 1000:
            raise Exception(f"Config DataFrame row count::{in0.count()} exceeds max run count")

        for row in in0.collect():
            update_config = self.config.update_from_row_map(row, conf_to_column)
            _inputs = inDFs
            results.append(self.__run__(spark, update_config, *_inputs))

        return do_union(results)
