from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from validation_map.functions import *
from . import *
from .config import *


class TableIterator_1(MetaGemExec):

    def __init__(self, config):
        self.config = config
        super().__init__()

    def execute(self, spark: SparkSession, subgraph_config: SubgraphConfig) -> List[DataFrame]:
        Config.update(subgraph_config)
        df_source_map_1 = source_map_1(spark)
        df_apply_custom_logic = apply_custom_logic(spark, df_source_map_1)
        subgraph_config.update(Config)

    def apply(self, spark: SparkSession, in0: DataFrame, ) -> None:
        inDFs = []
        results = []
        conf_to_column = dict(
            [("validation", "validation"),  ("source_catalog", "source_catalog"),  ("source_schema", "source_schema"),              ("source_table", "source_table"),  ("column", "column"),  ("rule_type", "rule_type"),              ("logic", "logic")]
        )

        if in0.count() > 1000:
            raise Exception(f"Config DataFrame row count::{in0.count()} exceeds max run count")

        for row in in0.collect():
            update_config = self.config.update_from_row_map(row, conf_to_column)
            _inputs = inDFs
            results.append(self.__run__(spark, update_config, *_inputs))

        return do_union(results)
