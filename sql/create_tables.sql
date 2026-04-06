START TRANSACTION;

DROP TABLE IF EXISTS events CASCADE;
DROP TABLE IF EXISTS repositories CASCADE;

-- Dimension table: repositories
CREATE TABLE IF NOT EXISTS repositories (
    repo_id       BIGINT PRIMARY KEY,
    repo_name     TEXT NOT NULL,
    first_seen_at TIMESTAMP,
    language      VARCHAR(100)
);

-- Fact table: daily event aggregates per repository
CREATE TABLE IF NOT EXISTS events (
    event_type    VARCHAR(50) NOT NULL,
    repo_id       BIGINT      NOT NULL,
    event_date    DATE        NOT NULL,
    event_count   INTEGER     NOT NULL DEFAULT 0,
    unique_actors INTEGER     NOT NULL DEFAULT 0
);

COMMIT;
