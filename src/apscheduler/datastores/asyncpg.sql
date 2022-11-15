CREATE TYPE coalescepolicy AS ENUM (
    'earliest',
    'latest',
    'all'
);

CREATE TYPE joboutcome AS ENUM (
    'success',
    'error',
    'missed_start_deadline',
    'cancelled'
);

CREATE TABLE job_results (
    job_id uuid NOT NULL,
    outcome joboutcome NOT NULL,
    finished_at timestamp with time zone,
    expires_at timestamp with time zone NOT NULL,
    exception bytea,
    return_value bytea
);

CREATE TABLE jobs (
    id uuid NOT NULL,
    task_id character varying(500) NOT NULL,
    args bytea NOT NULL,
    kwargs bytea NOT NULL,
    schedule_id character varying(500),
    scheduled_fire_time timestamp with time zone,
    jitter interval(6),
    start_deadline timestamp with time zone,
    result_expiration_time interval(6),
    tags character varying[] NOT NULL,
    created_at timestamp with time zone NOT NULL,
    started_at timestamp with time zone,
    acquired_by character varying(500),
    acquired_until timestamp with time zone
);

CREATE TABLE metadata (
    schema_version integer NOT NULL
);

CREATE TABLE schedules (
    id character varying(500) NOT NULL,
    task_id character varying(500) NOT NULL,
    trigger bytea,
    args bytea,
    kwargs bytea,
    "coalesce" coalescepolicy NOT NULL,
    misfire_grace_time interval(6),
    max_jitter interval(6),
    tags character varying[] NOT NULL,
    next_fire_time timestamp with time zone,
    last_fire_time timestamp with time zone,
    acquired_by character varying(500),
    acquired_until timestamp with time zone
);

CREATE TABLE tasks (
    id character varying(500) NOT NULL,
    func character varying(500) NOT NULL,
    executor character varying(500) NOT NULL,
    state bytea,
    max_running_jobs integer,
    misfire_grace_time interval(6),
    running_jobs integer DEFAULT 0 NOT NULL
);

ALTER TABLE ONLY job_results
    ADD CONSTRAINT job_results_pkey PRIMARY KEY (job_id);

ALTER TABLE ONLY jobs
    ADD CONSTRAINT jobs_pkey PRIMARY KEY (id);

ALTER TABLE ONLY schedules
    ADD CONSTRAINT schedules_pkey PRIMARY KEY (id);

ALTER TABLE ONLY tasks
    ADD CONSTRAINT tasks_pkey PRIMARY KEY (id);

CREATE INDEX ix_job_results_expires_at ON job_results USING btree (expires_at);

CREATE INDEX ix_job_results_finished_at ON job_results USING btree (finished_at);

CREATE INDEX ix_jobs_task_id ON jobs USING btree (task_id);

CREATE INDEX ix_schedules_next_fire_time ON schedules USING btree (next_fire_time);

CREATE INDEX ix_schedules_task_id ON schedules USING btree (task_id);
