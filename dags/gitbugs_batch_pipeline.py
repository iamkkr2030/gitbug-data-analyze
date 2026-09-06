import hashlib
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.python import get_current_context


def record_failure(context):
    from pipeline.database import mark_failed
    batch = context['ti'].xcom_pull(task_ids='register_batch')
    if batch:
        mark_failed(batch, context.get('exception', 'Task failed'))


def guarded(action, batch):
    if batch.get('skip'):
        return
    try:
        action(batch)
    except ValueError as error:
        raise AirflowFailException(str(error)) from error


@dag(dag_id='gitbugs_batch_pipeline', schedule=None,
     start_date=pendulum.datetime(2026, 1, 1, tz='UTC'), catchup=False,
     max_active_runs=1, tags=['gitbugs', 'spark'],
     default_args={'retries': 2, 'retry_delay': timedelta(seconds=30),
                   'execution_timeout': timedelta(minutes=30),
                   'on_failure_callback': record_failure})
def gitbugs_batch_pipeline():
    @task
    def inspect_source():
        import csv
        from pipeline.config import source_path
        from pipeline.source import inspect_source as inspect
        try:
            return inspect(source_path())
        except (ValueError, UnicodeError, csv.Error) as error:
            raise AirflowFailException(str(error)) from error

    @task
    def register_batch(source):
        from pipeline.database import register_batch as register
        owner = hashlib.sha256(get_current_context()['run_id'].encode()).hexdigest()[:32]
        return register(source, owner=owner)

    @task
    def archive_raw(batch):
        from pipeline.tasks import archive
        guarded(archive, batch)

    @task
    def normalize_with_spark(batch):
        from pipeline.tasks import normalize
        guarded(normalize, batch)

    @task
    def validate_silver(batch):
        from pipeline.tasks import metrics_for
        guarded(metrics_for, batch)

    @task
    def load_staging(batch):
        from pipeline.tasks import load
        guarded(load, batch)

    @task
    def publish_snapshot(batch):
        from pipeline.loading import publish
        from pipeline.tasks import metrics_for
        guarded(lambda b: publish(b, metrics_for(b)), batch)

    @task
    def verify_publish(batch):
        from pipeline.database import verify_batch
        verify_batch(batch['batch_id'])

    batch = register_batch(inspect_source())
    archive_raw(batch) >> normalize_with_spark(batch) >> validate_silver(batch) >> load_staging(batch) >> publish_snapshot(batch) >> verify_publish(batch)


gitbugs_batch_pipeline()
