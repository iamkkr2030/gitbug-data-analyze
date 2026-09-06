-- Current published snapshot summary.
SELECT s.*, ROUND(s.reciprocal_pair_count / NULLIF(s.undirected_relation_count, 0), 4)
    AS reciprocal_pair_ratio
FROM mart_relation_summary s
JOIN pipeline_current c ON c.batch_id = s.batch_id
WHERE c.dataset_name = 'gitbugs';

-- Top 10 issues by unique neighbors, with deterministic ties.
SELECT issue_id, in_degree, out_degree, neighbor_count
FROM current_issue_degree
ORDER BY neighbor_count DESC, issue_id
LIMIT 10;

-- Degree distribution.
SELECT neighbor_count, COUNT(*) AS issue_count
FROM current_issue_degree GROUP BY neighbor_count ORDER BY neighbor_count;

-- Mutual links: count each pair once.
SELECT a.issue_id, a.duplicate_id
FROM current_issue_relation a
JOIN current_issue_relation b
    ON a.issue_id = b.duplicate_id AND a.duplicate_id = b.issue_id
WHERE a.issue_id < a.duplicate_id
ORDER BY a.issue_id, a.duplicate_id;

-- Batch audit and duration (seconds).
SELECT batch_id, status, input_rows, output_rows,
       TIMESTAMPDIFF(SECOND, started_at, finished_at) AS elapsed_seconds, error_message
FROM pipeline_batch ORDER BY started_at DESC;

-- Query-plan evidence for indexed reverse lookups.
EXPLAIN SELECT * FROM current_issue_relation WHERE duplicate_id = 13357630;
