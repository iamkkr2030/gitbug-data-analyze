import hashlib
import re
import uuid
from contextlib import contextmanager

from pipeline.config import ROOT, TRANSFORM_VERSION, data_root, mysql_config


def connect():
    import pymysql
    return pymysql.connect(**mysql_config(), cursorclass=pymysql.cursors.DictCursor)


@contextmanager
def locked_connection():
    """Session lock survives commits; serializes CLI and DAG mutations on this database."""
    connection = connect()
    lock_name = "gitbugs:" + hashlib.sha256(mysql_config()["database"].encode()).hexdigest()[:40]
    acquired = False
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT GET_LOCK(%s, 30) AS acquired", (lock_name,))
            acquired = cursor.fetchone()["acquired"] == 1
        if not acquired:
            raise RuntimeError("Another GitBugs task holds the database lock; retry later")
        yield connection
    finally:
        try:
            connection.rollback()
            if acquired:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
        finally:
            connection.close()


def init_schema():
    with locked_connection() as connection, connection.cursor() as cursor:
        for statement in (ROOT / "sql" / "001_schema.sql").read_text(encoding="utf-8").split(";"):
            if statement.strip():
                cursor.execute(statement)
        connection.commit()


def register_batch(source, version=TRANSFORM_VERSION, owner=None):
    if not re.fullmatch(r"[A-Za-z0-9._-]{1,64}", version):
        raise ValueError("Transform version must contain 1-64 letters, digits, dots, hyphens or underscores")
    owner = owner or uuid.uuid4().hex
    if not re.fullmatch(r"[a-f0-9]{32}", owner):
        raise ValueError("Invalid owner token")
    batch_id = hashlib.sha256(f"{source['source_sha256']}:{version}".encode()).hexdigest()[:32]
    batch_dir = data_root() / "batches" / batch_id
    context = {**source, "batch_id": batch_id, "owner_token": owner, "transform_version": version,
               "raw_path": str(batch_dir / "raw" / "source.csv"),
               "work_dir": str(batch_dir / "attempts" / owner), "skip": False}
    with locked_connection() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT * FROM pipeline_batch WHERE batch_id=%s FOR UPDATE", (batch_id,))
        existing = cursor.fetchone()
        if existing and existing["status"] == "SUCCESS":
            context["skip"] = True
            return context
        if existing and existing["status"] == "RUNNING" and existing["owner_token"] != owner:
            raise RuntimeError(f"Batch {batch_id} is already running. Recover only after its worker has stopped.")
        if existing:
            cursor.execute("""UPDATE pipeline_batch SET owner_token=%s, status='RUNNING',
                started_at=UTC_TIMESTAMP(6), finished_at=NULL, error_message=NULL, output_rows=NULL
                WHERE batch_id=%s""", (owner, batch_id))
        else:
            cursor.execute("""INSERT INTO pipeline_batch
                (batch_id, source_sha256, transform_version, owner_token, status, started_at, input_rows)
                VALUES (%s,%s,%s,%s,'RUNNING',UTC_TIMESTAMP(6),%s)""",
                           (batch_id, source["source_sha256"], version, owner, source["input_rows"]))
        connection.commit()
    return context


def assert_owner(connection, context, allow_success=False):
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM pipeline_batch WHERE batch_id=%s FOR UPDATE", (context["batch_id"],))
        row = cursor.fetchone()
    if not row or row["owner_token"] != context["owner_token"]:
        raise RuntimeError("Batch ownership changed; refusing a stale worker write")
    if row["status"] != "RUNNING" and not (allow_success and row["status"] == "SUCCESS"):
        raise RuntimeError(f"Batch is {row['status']}, not RUNNING")
    return row


def mark_failed(context, error):
    if not context or context.get("skip"):
        return
    with locked_connection() as connection, connection.cursor() as cursor:
        cursor.execute("""UPDATE pipeline_batch SET status='FAILED', finished_at=UTC_TIMESTAMP(6),
            error_message=%s WHERE batch_id=%s AND owner_token=%s AND status='RUNNING'""",
                       (str(error)[:4000], context["batch_id"], context["owner_token"]))
        connection.commit()


def recover_batch(batch_id):
    """Explicit operator action after an abruptly terminated worker; fences stale contexts."""
    with locked_connection() as connection, connection.cursor() as cursor:
        cursor.execute("""UPDATE pipeline_batch SET status='FAILED', owner_token=%s,
            finished_at=UTC_TIMESTAMP(6), error_message='Operator recovered stopped worker'
            WHERE batch_id=%s AND status='RUNNING'""", (uuid.uuid4().hex, batch_id))
        changed = cursor.rowcount
        connection.commit()
    return changed


def verify_batch(batch_id):
    with connect() as connection, connection.cursor() as cursor:
        cursor.execute("""SELECT b.status, b.output_rows, s.* FROM pipeline_batch b
            JOIN mart_relation_summary s ON s.batch_id=b.batch_id WHERE b.batch_id=%s""", (batch_id,))
        row = cursor.fetchone()
        if not row or row["status"] != "SUCCESS":
            raise ValueError("Batch is not successfully published")
        cursor.execute("SELECT COUNT(*) AS n FROM issue_relation WHERE batch_id=%s", (batch_id,))
        if cursor.fetchone()["n"] != row["output_rows"] or row["output_rows"] != row["directed_relation_count"]:
            raise ValueError("Published relation count mismatch")
        cursor.execute("""SELECT COUNT(*) AS n, COALESCE(SUM(in_degree),0) AS incoming,
            COALESCE(SUM(out_degree),0) AS outgoing, COALESCE(SUM(neighbor_count),0) AS neighbors
            FROM mart_issue_degree WHERE batch_id=%s""", (batch_id,))
        degree = cursor.fetchone()
        if (degree["n"] != row["issue_count"] or degree["incoming"] != row["output_rows"]
                or degree["outgoing"] != row["output_rows"]
                or degree["neighbors"] != 2 * row["undirected_relation_count"]):
            raise ValueError("Published mart count mismatch")
        return row

