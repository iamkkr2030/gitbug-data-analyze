CREATE TABLE IF NOT EXISTS pipeline_batch (
    batch_id CHAR(32) PRIMARY KEY,
    source_sha256 CHAR(64) NOT NULL,
    transform_version VARCHAR(64) NOT NULL,
    owner_token CHAR(32) NOT NULL,
    status ENUM('RUNNING', 'FAILED', 'SUCCESS') NOT NULL,
    started_at DATETIME(6) NOT NULL,
    finished_at DATETIME(6) NULL,
    input_rows BIGINT NOT NULL,
    output_rows BIGINT NULL,
    error_message TEXT NULL,
    UNIQUE KEY uq_source_version (source_sha256, transform_version)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS stg_issue_relation (
    batch_id CHAR(32) NOT NULL,
    issue_id BIGINT NOT NULL,
    duplicate_id BIGINT NOT NULL,
    PRIMARY KEY (batch_id, issue_id, duplicate_id),
    CONSTRAINT ck_stg_ids CHECK (issue_id > 0 AND duplicate_id > 0 AND issue_id <> duplicate_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS issue_relation (
    batch_id CHAR(32) NOT NULL,
    issue_id BIGINT NOT NULL,
    duplicate_id BIGINT NOT NULL,
    PRIMARY KEY (batch_id, issue_id, duplicate_id),
    KEY ix_relation_reverse (batch_id, duplicate_id),
    CONSTRAINT fk_relation_batch FOREIGN KEY (batch_id) REFERENCES pipeline_batch(batch_id),
    CONSTRAINT ck_relation_ids CHECK (issue_id > 0 AND duplicate_id > 0 AND issue_id <> duplicate_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS stg_issue_degree (
    batch_id CHAR(32) NOT NULL,
    issue_id BIGINT NOT NULL,
    in_degree BIGINT NOT NULL,
    out_degree BIGINT NOT NULL,
    neighbor_count BIGINT NOT NULL,
    PRIMARY KEY (batch_id, issue_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS mart_issue_degree (
    batch_id CHAR(32) NOT NULL,
    issue_id BIGINT NOT NULL,
    in_degree BIGINT NOT NULL,
    out_degree BIGINT NOT NULL,
    neighbor_count BIGINT NOT NULL,
    PRIMARY KEY (batch_id, issue_id),
    CONSTRAINT fk_degree_batch FOREIGN KEY (batch_id) REFERENCES pipeline_batch(batch_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS mart_relation_summary (
    batch_id CHAR(32) PRIMARY KEY,
    issue_count BIGINT NOT NULL,
    directed_relation_count BIGINT NOT NULL,
    undirected_relation_count BIGINT NOT NULL,
    reciprocal_pair_count BIGINT NOT NULL,
    CONSTRAINT fk_summary_batch FOREIGN KEY (batch_id) REFERENCES pipeline_batch(batch_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS pipeline_current (
    dataset_name VARCHAR(64) PRIMARY KEY,
    batch_id CHAR(32) NOT NULL,
    CONSTRAINT fk_current_batch FOREIGN KEY (batch_id) REFERENCES pipeline_batch(batch_id)
) ENGINE=InnoDB;

CREATE OR REPLACE VIEW current_issue_relation AS
SELECT r.* FROM issue_relation r
JOIN pipeline_current c ON r.batch_id = c.batch_id
WHERE c.dataset_name = 'gitbugs';

CREATE OR REPLACE VIEW current_issue_degree AS
SELECT d.* FROM mart_issue_degree d
JOIN pipeline_current c ON d.batch_id = c.batch_id
WHERE c.dataset_name = 'gitbugs';

