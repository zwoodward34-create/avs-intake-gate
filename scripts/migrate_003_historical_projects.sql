-- Migration 003: historical_projects
-- Run in Supabase SQL Editor once.

CREATE TABLE IF NOT EXISTS historical_projects (
    id               BIGSERIAL PRIMARY KEY,
    project_name     TEXT,
    location         TEXT,
    year_completed   INTEGER,
    project_type     TEXT,
    material         TEXT,
    roof             TEXT,
    lfrs             TEXT,
    ahj              TEXT,
    notes            TEXT,
    raw_description  TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Optional: allow the service-role key to bypass RLS for backfill inserts
ALTER TABLE historical_projects ENABLE ROW LEVEL SECURITY;

CREATE POLICY "service role full access" ON historical_projects
    FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');
