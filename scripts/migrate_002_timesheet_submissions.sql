-- Migration 002: timesheet_submissions table
-- Run this in Supabase SQL Editor if the table doesn't already exist.
-- Safe to re-run: uses IF NOT EXISTS throughout.

CREATE TABLE IF NOT EXISTS timesheet_submissions (
    id              BIGSERIAL PRIMARY KEY,
    engineer_initials TEXT NOT NULL,
    period_start    DATE NOT NULL,
    period_end      DATE NOT NULL,
    status          TEXT NOT NULL DEFAULT 'DRAFT'
                        CHECK (status IN ('DRAFT','SUBMITTED','APPROVED','REJECTED')),
    total_hours     NUMERIC(6,2) NOT NULL DEFAULT 0,
    submitted_at    TIMESTAMPTZ,
    reviewed_at     TIMESTAMPTZ,
    reviewer_notes  TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (engineer_initials, period_start)
);

-- Index for the status query used by get_review_queue()
CREATE INDEX IF NOT EXISTS idx_ts_submissions_status
    ON timesheet_submissions (status);

-- Index for the per-engineer period lookup
CREATE INDEX IF NOT EXISTS idx_ts_submissions_engineer_period
    ON timesheet_submissions (engineer_initials, period_start, period_end);
