"""Shared steps for the CLI and Airflow. Only small metadata crosses task boundaries."""
import json
from datetime import datetime, timezone
from pathlib import Path

from pipeline.quality import validate_metrics
from pipeline.source import archive_source, write_json


def archive(context):
    archive_source(context)
    manifest = {
        k: context[k] for k in ('source_path', 'source_sha256', 'source_bytes',
                               'input_rows', 'batch_id', 'transform_version')}
    manifest['archived_at'] = datetime.now(timezone.utc).isoformat()
    write_json(Path(context['raw_path']).with_name('manifest.json'), manifest)


def normalize(context):
    from pipeline.spark_transform import create_spark, transform
    spark = create_spark()
    try:
        transform(context, spark)
    finally:
        spark.stop()


def metrics_for(context):
    metrics = json.loads((Path(context['work_dir']) / 'quality.json').read_text(encoding='utf-8'))
    for key in ('batch_id', 'source_sha256', 'transform_version', 'input_rows'):
        if metrics[key] != context[key]:
            raise ValueError(f'Quality report {key} mismatch')
    validate_metrics(metrics)
    return metrics


def load(context, failpoint=None):
    from pipeline.loading import stage_frames
    from pipeline.spark_transform import create_spark
    metrics = metrics_for(context)
    spark = create_spark()
    try:
        silver = Path(context['work_dir']) / 'silver'
        relations = spark.read.parquet(str(silver / 'relations'))
        degrees = spark.read.parquet(str(silver / 'degrees'))
        stage_frames(context, relations, degrees, metrics, failpoint=failpoint)
    finally:
        spark.stop()


def run(source, version, failpoint=None):
    from pipeline.database import register_batch, mark_failed, verify_batch
    from pipeline.loading import publish
    from pipeline.source import inspect_source
    context = register_batch(inspect_source(source), version)
    if context['skip']:
        return {'batch_id': context['batch_id'], 'skipped': True, **verify_batch(context['batch_id'])}
    try:
        archive(context)
        normalize(context)
        metrics = metrics_for(context)
        load(context, failpoint)
        publish(context, metrics, failpoint)
        return {'skipped': False, **verify_batch(context['batch_id'])}
    except Exception as error:
        mark_failed(context, error)
        raise
