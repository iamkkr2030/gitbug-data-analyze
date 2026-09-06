from itertools import islice

from pipeline.database import assert_owner, locked_connection
from pipeline.quality import validate_metrics


def chunks(rows, size):
    iterator = iter(rows)
    while chunk := list(islice(iterator, size)):
        yield chunk


def stage_frames(context, relations, degrees, metrics, failpoint=None, chunk_size=500):
    """Bounded driver-side inserts. Partial commits are isolated from published data."""
    validate_metrics(metrics)
    batch_id = context["batch_id"]
    with locked_connection() as connection, connection.cursor() as cursor:
        assert_owner(connection, context)
        for table in ("stg_issue_relation", "stg_issue_degree"):
            cursor.execute(f"DELETE FROM {table} WHERE batch_id=%s", (batch_id,))
        connection.commit()
        for table, columns, frame in (
            ("stg_issue_relation", ["issue_id", "duplicate_id"], relations),
            ("stg_issue_degree", ["issue_id", "in_degree", "out_degree", "neighbor_count"], degrees),
        ):
            placeholders = ",".join(["%s"] * (len(columns) + 1))
            sql = f"INSERT INTO {table} (batch_id,{','.join(columns)}) VALUES ({placeholders})"
            records = ((batch_id, *(int(row[c]) for c in columns)) for row in frame.toLocalIterator())
            for part in chunks(records, chunk_size):
                cursor.executemany(sql, part)
                connection.commit()
                if failpoint == "after_staging_chunk":
                    raise RuntimeError("Injected failure after a committed staging chunk")
        validate_staging(cursor, batch_id, metrics)


def validate_staging(cursor, batch_id, metrics):
    cursor.execute("SELECT COUNT(*) AS n FROM stg_issue_relation WHERE batch_id=%s", (batch_id,))
    if cursor.fetchone()["n"] != metrics["output_rows"]:
        raise ValueError("Staging relation count mismatch")
    cursor.execute("""SELECT COUNT(*) AS n FROM (
        SELECT LEAST(issue_id,duplicate_id), GREATEST(issue_id,duplicate_id)
        FROM stg_issue_relation WHERE batch_id=%s
        GROUP BY LEAST(issue_id,duplicate_id), GREATEST(issue_id,duplicate_id)
        ) pairs""", (batch_id,))
    if cursor.fetchone()["n"] != metrics["undirected_relation_count"]:
        raise ValueError("Staging undirected count mismatch")
    if metrics["reciprocal_pair_count"] != metrics["output_rows"] - metrics["undirected_relation_count"]:
        raise ValueError("Reciprocal pair count mismatch")
    cursor.execute("""SELECT COUNT(*) AS n, COALESCE(SUM(in_degree),0) AS incoming,
        COALESCE(SUM(out_degree),0) AS outgoing, COALESCE(SUM(neighbor_count),0) AS neighbors
        FROM stg_issue_degree WHERE batch_id=%s""", (batch_id,))
    row = cursor.fetchone()
    if (row["n"] != metrics["issue_count"] or row["incoming"] != metrics["output_rows"]
            or row["outgoing"] != metrics["output_rows"]
            or row["neighbors"] != 2 * metrics["undirected_relation_count"]):
        raise ValueError("Staging degree metrics mismatch")


def publish(context, metrics, failpoint=None):
    validate_metrics(metrics)
    batch_id = context["batch_id"]
    with locked_connection() as connection, connection.cursor() as cursor:
        row = assert_owner(connection, context, allow_success=True)
        if row["status"] == "SUCCESS":
            return  # A retry after a successful commit must not move the current pointer backwards.
        validate_staging(cursor, batch_id, metrics)
        for table in ("issue_relation", "mart_issue_degree", "mart_relation_summary"):
            cursor.execute(f"DELETE FROM {table} WHERE batch_id=%s", (batch_id,))
        cursor.execute("""INSERT INTO issue_relation (batch_id,issue_id,duplicate_id)
            SELECT batch_id,issue_id,duplicate_id FROM stg_issue_relation WHERE batch_id=%s""", (batch_id,))
        cursor.execute("""INSERT INTO mart_issue_degree (batch_id,issue_id,in_degree,out_degree,neighbor_count)
            SELECT batch_id,issue_id,in_degree,out_degree,neighbor_count
            FROM stg_issue_degree WHERE batch_id=%s""", (batch_id,))
        cursor.execute("""INSERT INTO mart_relation_summary
            (batch_id,issue_count,directed_relation_count,undirected_relation_count,reciprocal_pair_count)
            VALUES (%s,%s,%s,%s,%s)""", (batch_id, metrics["issue_count"], metrics["output_rows"],
                                        metrics["undirected_relation_count"], metrics["reciprocal_pair_count"]))
        cursor.execute("""UPDATE pipeline_batch SET status='SUCCESS', output_rows=%s,
            finished_at=UTC_TIMESTAMP(6), error_message=NULL WHERE batch_id=%s""", (metrics["output_rows"], batch_id))
        cursor.execute("""INSERT INTO pipeline_current (dataset_name,batch_id) VALUES ('gitbugs',%s)
            ON DUPLICATE KEY UPDATE batch_id=%s""", (batch_id, batch_id))
        if failpoint == "before_commit":
            raise RuntimeError("Injected failure before publish commit")
        connection.commit()

