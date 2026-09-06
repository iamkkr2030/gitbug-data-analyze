import json
import os
import time
from pathlib import Path

from pipeline.source import sha256_file, write_json


def create_spark():
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.appName("gitbugs-batch")
             .master(os.environ.get("SPARK_MASTER", "local[2]"))
             .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4"))
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def normalized_frames(raw):
    from pyspark.sql import functions as F
    # Canonicalize leading zeros before BIGINT conversion, including arbitrarily long zero prefixes.
    strip = lambda column: F.regexp_replace(column, r"(?U)^\s+|\s+$", "")
    exploded = raw.select(strip(F.coalesce(F.col("Issue id"), F.lit(""))).alias("issue_raw"),
                          F.explode(F.split(F.coalesce(F.col("Duplicate id"), F.lit("")), ",", -1))
                          .alias("duplicate_raw"))
    exploded = exploded.withColumn("duplicate_raw", strip(F.col("duplicate_raw")))
    for source, target in (("issue_raw", "issue_id"), ("duplicate_raw", "duplicate_id")):
        significant = F.regexp_replace(F.col(source), "^0+", "")
        valid = (F.col(source).rlike("^[0-9]+$") & (F.length(significant) > 0)
                 & ((F.length(significant) < 19)
                    | ((F.length(significant) == 19) & (significant <= F.lit("9223372036854775807")))))
        exploded = exploded.withColumn(target, F.when(valid, significant.cast("long")))
    exploded = exploded.withColumn("reason",
        F.when(F.col("issue_id").isNull() | F.col("duplicate_id").isNull(), F.lit("invalid_id"))
         .when(F.col("issue_id") == F.col("duplicate_id"), F.lit("self_reference")))
    rejected = exploded.filter(F.col("reason").isNotNull())
    valid = exploded.filter(F.col("reason").isNull()).select("issue_id", "duplicate_id")
    return exploded, rejected, valid.dropDuplicates(["issue_id", "duplicate_id"])


def aggregate_frames(relations):
    from pyspark.sql import functions as F
    undirected = relations.select(F.least("issue_id", "duplicate_id").alias("a"),
                                  F.greatest("issue_id", "duplicate_id").alias("b")).distinct()
    ids = relations.select("issue_id").union(relations.select(F.col("duplicate_id").alias("issue_id"))).distinct()
    incoming = relations.groupBy("duplicate_id").count().select(F.col("duplicate_id").alias("issue_id"),
                                                               F.col("count").alias("in_degree"))
    outgoing = relations.groupBy("issue_id").count().withColumnRenamed("count", "out_degree")
    neighbors = (undirected.select(F.col("a").alias("issue_id"))
                 .union(undirected.select(F.col("b").alias("issue_id")))
                 .groupBy("issue_id").count().withColumnRenamed("count", "neighbor_count"))
    degrees = (ids.join(incoming, "issue_id", "left").join(outgoing, "issue_id", "left")
               .join(neighbors, "issue_id", "left").fillna(0))
    return undirected, degrees


def transform(context, spark):
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType
    started = time.perf_counter()
    if sha256_file(context["raw_path"]) != context["source_sha256"]:
        raise ValueError("Archived input hash mismatch")
    schema = StructType([StructField("Issue id", StringType()), StructField("Duplicate id", StringType())])
    raw = (spark.read.schema(schema).option("header", True).option("enforceSchema", False)
           .option("mode", "FAILFAST").option("multiLine", True).option("escape", '"')
           .csv(context["raw_path"]).cache())
    expanded, rejected, relations = normalized_frames(raw)
    expanded.cache()
    relations.cache()
    undirected, degrees = aggregate_frames(relations)
    degrees.cache()
    try:
        input_rows, expanded_rows, quarantined_rows, output_rows = (
            raw.count(), expanded.count(), rejected.count(), relations.count())
        if input_rows != context["input_rows"]:
            raise ValueError("Spark CSV row count differs from strict source inspection")
        work = Path(context["work_dir"])
        relations.write.mode("overwrite").parquet(str(work / "silver" / "relations"))
        degrees.write.mode("overwrite").parquet(str(work / "silver" / "degrees"))
        rejected.write.mode("overwrite").json(str(work / "quarantine"))
        undirected_count = undirected.count()
        # For unique directed edges, E - U equals the number of mutual unordered pairs.
        metrics = {"batch_id": context["batch_id"], "source_sha256": context["source_sha256"],
                   "transform_version": context["transform_version"], "engine": "spark",
                   "spark_version": spark.version, "input_rows": input_rows,
                   "expanded_rows": expanded_rows, "quarantined_rows": quarantined_rows,
                   "duplicates_removed": expanded_rows - quarantined_rows - output_rows,
                   "output_rows": output_rows, "issue_count": degrees.count(),
                   "undirected_relation_count": undirected_count,
                   "reciprocal_pair_count": output_rows - undirected_count,
                   "transform_seconds": round(time.perf_counter() - started, 4)}
        metrics["top_issues"] = [row.asDict() for row in degrees.orderBy(F.desc("neighbor_count"), "issue_id").limit(10).collect()]
        write_json(work / "quality.json", metrics)
        plan = relations._jdf.queryExecution().toString()
        (work / "spark-plan.txt").write_text(plan, encoding="utf-8")
        return metrics
    finally:
        raw.unpersist()
        expanded.unpersist()
        relations.unpersist()
        degrees.unpersist()
