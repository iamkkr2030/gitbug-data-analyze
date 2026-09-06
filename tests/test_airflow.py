import pytest

pytestmark = pytest.mark.airflow
pytest.importorskip('airflow')


def test_dag_import_and_order():
    from airflow.models import DagBag
    from pipeline.config import ROOT
    bag = DagBag(str(ROOT / 'dags'), include_examples=False)
    assert not bag.import_errors
    dag = bag.get_dag('gitbugs_batch_pipeline')
    expected = ['inspect_source', 'register_batch', 'archive_raw', 'normalize_with_spark',
                'validate_silver', 'load_staging', 'publish_snapshot', 'verify_publish']
    assert {task.task_id for task in dag.tasks} == set(expected)
    for first, second in zip(expected, expected[1:]):
        assert second in dag.get_task(first).downstream_task_ids
    assert dag.max_active_runs == 1
    assert dag.schedule_interval is None
