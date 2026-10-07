-- Create tables for the job market data warehouse

-- Raw jobs table (staging)
CREATE TABLE IF NOT EXISTS raw_jobs (
    id SERIAL PRIMARY KEY,
    job_title VARCHAR(500),
    company VARCHAR(500),
    location VARCHAR(500),
    salary VARCHAR(200),
    job_type VARCHAR(100),
    experience_level VARCHAR(100),
    skills TEXT,
    posted_date VARCHAR(100),
    source VARCHAR(200),
    description TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Cleaned/transformed jobs table
CREATE TABLE IF NOT EXISTS transformed_jobs (
    id SERIAL PRIMARY KEY,
    job_title VARCHAR(500),
    company VARCHAR(500),
    city VARCHAR(200),
    state VARCHAR(200),
    country VARCHAR(200),
    salary_min NUMERIC,
    salary_max NUMERIC,
    salary_avg NUMERIC,
    salary_currency VARCHAR(10) DEFAULT 'USD',
    job_type VARCHAR(100),
    experience_level VARCHAR(100),
    posted_date DATE,
    source VARCHAR(200),
    description TEXT,
    transformed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Skills dimension table
CREATE TABLE IF NOT EXISTS job_skills (
    id SERIAL PRIMARY KEY,
    job_id INTEGER REFERENCES transformed_jobs(id),
    skill VARCHAR(200),
    skill_category VARCHAR(100)
);

-- Pipeline run log table
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(100),
    dag_id VARCHAR(100),
    task_id VARCHAR(100),
    status VARCHAR(50),
    records_processed INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    error_message TEXT,
    started_at TIMESTAMP,
    completed_at TIMESTAMP
);

-- Analytics summary table
CREATE TABLE IF NOT EXISTS analytics_summary (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(200),
    metric_value NUMERIC,
    dimension VARCHAR(200),
    dimension_value VARCHAR(500),
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_transformed_jobs_city ON transformed_jobs(city);
CREATE INDEX IF NOT EXISTS idx_transformed_jobs_type ON transformed_jobs(job_type);
CREATE INDEX IF NOT EXISTS idx_transformed_jobs_exp ON transformed_jobs(experience_level);
CREATE INDEX IF NOT EXISTS idx_job_skills_skill ON job_skills(skill);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs(status);
