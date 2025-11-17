from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from validation_keep.functions import *

def bronze_distinct_counts(spark: SparkSession, in0: DataFrame, in1: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F, types as T
    # target columns
    target_cols = [r["col_name"] for r in in1.select("col_name").distinct().collect()]

    if not target_cols:
        return spark.createDataFrame([], "col_name string, bronze_distinct_count long")

    # 1) Normalize types once (dates/timestamps → yyyy-MM-dd, others → string)
    dtype = {f.name: f\
        .dataType for f in in0.schema.fields}
    select_exprs = []

    for c in target_cols:

        if c not in dtype:
            continue

        col = F.col(c)

        if isinstance(dtype[c], (T.DateType, T.TimestampType)):
            col = F.date_format(col, "yyyy-MM-dd")
        else:
            col = col.cast("string")

        select_exprs.append(col.alias(c))

    if not select_exprs:
        return spark.createDataFrame([], "col_name string, bronze_distinct_count long")

    normalized = in0.select(*select_exprs)
    # 2) Single-pass aggregation across all columns
    agg_exprs = [F.approx_count_distinct(F.col(c)).alias(c) for c in normalized.columns]
    wide_counts = normalized.agg(*agg_exprs) # one row, one field per column
    # 3) Convert wide → long without SQL
    counts_map = wide_counts.select(
        F\
          .map_from_arrays(
            F.array([F.lit(c) for c in normalized.columns]),
            F.array([F.col(c).cast("long") for c in normalized.columns])
          )\
          .alias("m")
    )
    out0 = counts_map.select(F.explode("m").alias("column_name", "bronze_distinct_count"))

    return out0
