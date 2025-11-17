from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from validation_keep.functions import *
from . import *
from .config import *


class iterator_validation_keep(MetaGemExec):

    def __init__(self, config):
        self.config = config
        super().__init__()

    def execute(self, spark: SparkSession, subgraph_config: SubgraphConfig) -> List[DataFrame]:
        Config.update(subgraph_config)
        df_source_bronze_table = source_bronze_table(spark)
        df_bronze_get_columns = bronze_get_columns(spark, df_source_bronze_table)
        df_bronze_parse_columns = bronze_parse_columns(spark, df_bronze_get_columns)
        df_source_silver_table = source_silver_table(spark)
        df_silver_get_columns = silver_get_columns(spark, df_source_silver_table)
        df_silver_parse_columns = silver_parse_columns(spark, df_silver_get_columns)
        df_join_bronze_silver_columns = join_bronze_silver_columns(
            spark, 
            df_bronze_parse_columns, 
            df_silver_parse_columns
        )
        df_bronze_distinct_counts = bronze_distinct_counts(spark, df_source_bronze_table, df_bronze_get_columns)
        df_inner_join_columns_bronze_counts = inner_join_columns_bronze_counts(
            spark, 
            df_bronze_distinct_counts, 
            df_join_bronze_silver_columns
        )
        df_silver_distinct_counts = silver_distinct_counts(spark, df_silver_get_columns, df_source_silver_table)
        df_join_columns_counts = join_columns_counts(
            spark, 
            df_inner_join_columns_bronze_counts, 
            df_silver_distinct_counts
        )
        df_evaluate_keep_logic = evaluate_keep_logic(spark, df_join_columns_counts)
        df_compile_keep_rule = compile_keep_rule(spark, df_evaluate_keep_logic)
        subgraph_config.update(Config)

    def apply(self, spark: SparkSession, in0: DataFrame, ) -> None:
        inDFs = []
        results = []
        conf_to_column = dict(
            [("bronze_catalog", "bronze_catalog"),  ("bronze_schema", "bronze_schema"),  ("bronze_table", "bronze_table"),              ("bronze_path", "bronze_path"),  ("silver_catalog", "silver_catalog"),              ("silver_schema", "silver_schema"),  ("silver_table", "silver_table"),              ("silver_path", "silver_path"),  ("suppress_columns", "suppress_columns"),  ("keep", "keep"),              ("supress", "supress"),  ("map", "map"),  ("sha256", "sha256"),  ("mask", "mask")]
        )

        if in0.count() > 1000:
            raise Exception(f"Config DataFrame row count::{in0.count()} exceeds max run count")

        for row in in0.collect():
            update_config = self.config.update_from_row_map(row, conf_to_column)
            _inputs = inDFs
            results.append(self.__run__(spark, update_config, *_inputs))

        return do_union(results)
