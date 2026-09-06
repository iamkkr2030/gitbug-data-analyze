import csv

import pytest

from pipeline.config import MAX_ID, ROOT
from pipeline.quality import parse_id, reference_profile, validate_metrics
from pipeline.source import archive_source, inspect_source


@pytest.mark.parametrize('text,expected', [
    (' 001 ', 1), ('0' * 5000 + '7', 7), (str(MAX_ID), MAX_ID),
    ('', None), ('0', None), ('-1', None), ('1.5', None), ('1e3', None),
    ('１２', None), (str(MAX_ID + 1), None), ('9' * 5000, None),
])
def test_id_boundaries(text, expected):
    assert parse_id(text) == expected


def test_repository_profile():
    report = reference_profile(ROOT / 'gitbugs_full_data.csv')
    assert [report[k] for k in ('input_rows', 'expanded_rows', 'duplicates_removed',
                              'output_rows', 'issue_count', 'undirected_relation_count')] == [1021, 1042, 8, 1034, 1068, 560]
    validate_metrics(report)


def test_quality_gate_and_direction(tmp_path):
    source = tmp_path / 'source.csv'
    source.write_text('Issue id,Duplicate id\n1,"2, 2, 1, x, 0, 9223372036854775808"\n2,1\n', encoding='utf-8')
    report = reference_profile(source)
    assert report['expanded_rows'] == 7
    assert report['quarantined_rows'] == 4
    assert report['duplicates_removed'] == 1
    assert report['output_rows'] == 2
    assert report['undirected_relation_count'] == 1
    assert report['top_issues'][0]['neighbor_count'] == 1
    with pytest.raises(ValueError, match='Quality gate'):
        validate_metrics(report)


@pytest.mark.parametrize('content', ['bad,header\n1,2\n', 'Issue id,Duplicate id\n1,2,3\n',
                                    'Issue id,Duplicate id\n1,"2\n', 'Issue id,Duplicate id\n'])
def test_malformed_csv(content, tmp_path):
    source = tmp_path / 'bad.csv'
    source.write_text(content, encoding='utf-8')
    with pytest.raises((ValueError, csv.Error)):
        inspect_source(source)


def test_archive_detects_source_change(tmp_path):
    source = tmp_path / 'source.csv'
    source.write_text('Issue id,Duplicate id\n1,2\n', encoding='utf-8')
    context = {**inspect_source(source), 'raw_path': str(tmp_path / 'raw.csv')}
    source.write_text('Issue id,Duplicate id\n1,3\n', encoding='utf-8')
    with pytest.raises(ValueError, match='Source changed'):
        archive_source(context)
    assert not (tmp_path / 'raw.csv').exists()


def test_reconciliation():
    report = reference_profile(ROOT / 'gitbugs_full_data.csv')
    report['output_rows'] -= 1
    with pytest.raises(ValueError, match='reconciliation'):
        validate_metrics(report)
