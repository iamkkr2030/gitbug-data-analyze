import pytest

pytestmark = pytest.mark.spark
pytest.importorskip('pyspark')

from pipeline.config import ROOT
from pipeline.source import inspect_source
from pipeline.spark_transform import create_spark, normalized_frames, aggregate_frames, transform


@pytest.fixture(scope='module')
def spark():
    session = create_spark()
    yield session
    session.stop()


def test_invalid_and_reciprocal(spark):
    raw = spark.createDataFrame([
        (' 001 ', '2,2,1,x,0,9223372036854775808,'), ('2', '1'),
        ('0' * 100 + '3', '9223372036854775807'), ('4', None),
    ], ['Issue id', 'Duplicate id'])
    expanded, rejected, relations = normalized_frames(raw)
    assert expanded.count() == 10
    assert rejected.count() == 6
    assert {tuple(r) for r in relations.collect()} == {(1, 2), (2, 1), (3, 9223372036854775807)}
    _, degrees = aggregate_frames(relations)
    assert degrees.filter('issue_id = 1').first().asDict() == {
        'issue_id': 1, 'in_degree': 1, 'out_degree': 1, 'neighbor_count': 1}


def test_actual_csv_parquet_round_trip(spark, tmp_path):
    source = ROOT / 'gitbugs_full_data.csv'
    context = {**inspect_source(source), 'batch_id': 'test', 'transform_version': 'v1',
               'raw_path': str(source), 'work_dir': str(tmp_path)}
    metrics = transform(context, spark)
    assert [metrics[k] for k in ('input_rows', 'expanded_rows', 'quarantined_rows',
                                'duplicates_removed', 'output_rows', 'issue_count',
                                'undirected_relation_count')] == [1021, 1042, 0, 8, 1034, 1068, 560]
    assert spark.read.parquet(str(tmp_path / 'silver' / 'relations')).count() == 1034
    assert (tmp_path / 'quality.json').exists()
