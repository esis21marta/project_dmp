CREATE TABLE aggregator_runs (
    run_id SERIAL PRIMARY KEY NOT NULL,
    status VARCHAR,
    engine VARCHAR,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    top_rank INTEGER,
    channel_limits VARCHAR
);

CREATE TABLE aggregator_job_files (
    id SERIAL PRIMARY KEY NOT NULL,
    file_name VARCHAR,
    file_path VARCHAR,
    status VARCHAR,
    engine VARCHAR,
    run_id INTEGER,
    temp_keyword_ids INTEGER[],
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    FOREIGN KEY (run_id) REFERENCES aggregator_runs (run_id)
);

CREATE TABLE aggregator_whitelist_files (
    whitelist_id SERIAL PRIMARY KEY NOT NULL,
    whitelist_filepath VARCHAR,
    run_id INTEGER,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    FOREIGN KEY (run_id) REFERENCES aggregator_runs (run_id)
);

