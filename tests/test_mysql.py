"""Uses only a dedicated database ending in _test; no production data is deleted."""
import os
import uuid

import pytest

pytestmark = pytest.mark.mysql
if not os.environ.get('MYSQL_DATABASE', '').endswith('_test'):
    pytest.skip('Set MYSQL_DATABASE to a dedicated *_test database', allow_module_level=True)
pytest.importorskip('pymysql')

from pipeline.database import connect, init_schema, register_batch, mark_failed, verify_batch
from pipeline.loading import stage_frames, publish
from pipeline.source import inspect_source


class Frame:
    def __init__(self, rows):
        self.rows = rows

    def toLocalIterator(self):
        return iter(self.rows)


def current():
    with connect() as db, db.cursor() as cursor:
        cursor.execute("SELECT batch_id FROM pipeline_current WHERE dataset_name='gitbugs'")
        row = cursor.fetchone()
        return row['batch_id'] if row else None


def fixture_batch(tmp_path, pairs):
    source = tmp_path / 'source.csv'
    source.write_text('Issue id,Duplicate id\n' + ''.join(f'{a},{b}\n' for a, b in pairs))
    version = uuid.uuid4().hex
    info = inspect_source(source)
    context = register_batch(info, version)
    ids = {i for p in pairs for i in p}
    degrees = Frame([{'issue_id': i, 'in_degree': sum(b == i for a, b in pairs),
                      'out_degree': sum(a == i for a, b in pairs),
                      'neighbor_count': len({b if a == i else a for a, b in pairs if i in (a, b)})}
                     for i in ids])
    undirected = len({tuple(sorted(p)) for p in pairs})
    metrics = {'input_rows': len(pairs), 'expanded_rows': len(pairs), 'quarantined_rows': 0,
               'duplicates_removed': 0, 'output_rows': len(pairs), 'issue_count': len(ids),
               'undirected_relation_count': undirected, 'reciprocal_pair_count': len(pairs) - undirected}
    relations = Frame([{'issue_id': a, 'duplicate_id': b} for a, b in pairs])
    return context, relations, degrees, metrics, info, version


@pytest.mark.parametrize('failpoint', ['after_staging_chunk', 'before_commit'])
def test_retry_is_atomic_and_idempotent(tmp_path, failpoint):
    init_schema()
    old, rel, deg, met, _, _ = fixture_batch(tmp_path, [(1, 2), (2, 1)])
    stage_frames(old, rel, deg, met)
    publish(old, met)
    batch, rel, deg, met, info, version = fixture_batch(tmp_path, [(3, 4)])
    with pytest.raises(RuntimeError, match='Injected failure'):
        stage_frames(batch, rel, deg, met, failpoint=failpoint, chunk_size=1)
        publish(batch, met, failpoint=failpoint)
    assert current() == old['batch_id']
    assert verify_batch(old['batch_id'])['output_rows'] == 2
    mark_failed(batch, 'injected')
    retried = register_batch(info, version)
    assert retried['batch_id'] == batch['batch_id']
    stage_frames(retried, rel, deg, met)
    publish(retried, met)
    assert verify_batch(retried['batch_id'])['output_rows'] == 1
    assert register_batch(info, version)['skip']
    publish(old, {'input_rows': 2, 'expanded_rows': 2, 'quarantined_rows': 0,
                  'duplicates_removed': 0, 'output_rows': 2})
    assert current() == retried['batch_id']
    with connect() as db, db.cursor() as cursor:
        cursor.execute('SELECT issue_id, duplicate_id FROM current_issue_relation')
        assert cursor.fetchall() == [{'issue_id': 3, 'duplicate_id': 4}]


def test_stale_worker_cannot_publish(tmp_path):
    init_schema()
    batch, rel, deg, met, info, version = fixture_batch(tmp_path, [(5, 6)])
    stage_frames(batch, rel, deg, met)
    mark_failed(batch, 'stopped')
    register_batch(info, version)
    with pytest.raises(RuntimeError, match='ownership'):
        publish(batch, met)
