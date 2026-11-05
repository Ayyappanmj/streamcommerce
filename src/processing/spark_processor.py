"""PySpark implementation of the read/clean stage (engine=spark).

Spark reads the landed CSV with an explicit all-string schema, casts each column to its declared type,
flags values that fail to cast (``_parse_errors``) and ranks rows per business key (``_dup_rank``).
The result is collected to pandas for the shared quality layer. Requires pyspark and Java 11/17.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from src.common.exceptions import MissingDependencyError
from src.common.schemas import DatasetSpec

log = logging.getLogger(__name__)


def get_spark():
    """Create (or reuse) a small local SparkSession."""
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise MissingDependencyError("pyspark is not installed (pip install pyspark==3.5.1; needs Java 17)") from exc
    return (SparkSession.builder.master("local[2]").appName("streamcommerce")
            .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4").getOrCreate())


def read_and_clean(path: Path, spec: DatasetSpec, spark=None) -> pd.DataFrame:
    """Return a typed pandas DataFrame with ``_parse_errors`` and ``_dup_rank`` columns."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    spark = spark or get_spark()
    schema = StructType([StructField(c, StringType(), True) for c in spec.column_names])
    raw = spark.read.csv(str(path), header=True, schema=schema, mode="DROPMALFORMED", enforceSchema=False,
                         ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True, emptyValue=None)
    cast_map = {"str": "string", "float": "double", "int": "int", "datetime": "timestamp"}
    flags = []
    typed = raw
    for col, typ in spec.columns.items():
        casted = F.to_timestamp(F.col(col)) if typ == "datetime" else F.col(col).cast(cast_map[typ])
        flags.append(F.when(F.col(col).isNotNull() & casted.isNull(), F.lit(col)))
        typed = typed.withColumn(col, casted)
    typed = typed.withColumn("_parse_errors", F.concat_ws(",", *flags))
    window = Window.partitionBy(spec.key).orderBy(F.monotonically_increasing_id())
    typed = typed.withColumn("_dup_rank", F.when(F.col(spec.key).isNull(), F.lit(1)).otherwise(F.row_number().over(window)))
    pdf = typed.toPandas()
    for col, typ in spec.columns.items():
        if typ == "datetime":
            pdf[col] = pd.to_datetime(pdf[col], utc=True)
        elif typ == "int":
            pdf[col] = pd.to_numeric(pdf[col]).astype("Int64")
    return pdf
