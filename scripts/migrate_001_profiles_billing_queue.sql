-- Migration 001: profiles + billing_queue
-- Run this in the Supabase SQL Editor (Dashboard → SQL Editor → New query)

-- ── profiles ─────────────────────────────────────────────────────────────────
-- One row per team member. Drives RBAC throughout the app.
-- role values: 'admin' | 'billing' | 'engineer' | 'drafter'

CREATE TABLE IF NOT EXISTS profiles (
    id                   SERIAL PRIMARY KEY,
    initials             TEXT        NOT NULL UNIQUE,
    full_name            TEXT,
    email                TEXT,
    role                 TEXT        NOT NULL DEFAULT 'engineer'
                         CHECK (role IN ('admin','billing','engineer','drafter')),
    color                TEXT,
    pool                 TEXT        CHECK (pool IN ('engineering','drafting')),
    capacity_multiplier  NUMERIC(4,2) NOT NULL DEFAULT 1.0,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed from existing hard-coded team data
INSERT INTO profiles (initials, role, color, pool, capacity_multiplier) VALUES
    ('MK', 'admin',    '#0891b2', 'engineering', 0.2),
    ('NK', 'billing',  '#d97706', 'engineering', 1.0),
    ('RO', 'engineer', '#9333ea', 'engineering', 1.0),
    ('JW', 'engineer', '#2563eb', 'engineering', 1.0),
    ('JR', 'engineer', '#16a34a', 'engineering', 0.8),
    ('JK', 'engineer', NULL,      'engineering', 0.8),
    ('RK', 'engineer', '#0d9488', 'engineering', 1.0),
    ('RS', 'drafter',  '#ea580c', 'drafting',    1.0),
    ('SW', 'engineer', '#7c3aed', 'drafting',    1.0),
    ('JP', 'drafter',  '#db2777', 'drafting',    0.8)
ON CONFLICT (initials) DO NOTHING;

-- ── billing_queue ─────────────────────────────────────────────────────────────
-- Natalie's billing workflow: one row per invoice event.
-- status values: 'pending' | 'sent' | 'paid' | 'overdue' | 'disputed' | 'voided'

CREATE TABLE IF NOT EXISTS billing_queue (
    id                  SERIAL PRIMARY KEY,
    intake_id           INTEGER     NOT NULL REFERENCES intakes(id) ON DELETE CASCADE,
    billing_phase_code  TEXT        NOT NULL,
    project_number      TEXT,
    client_name         TEXT,
    status              TEXT        NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending','sent','paid','overdue','disputed','voided')),
    invoice_number      TEXT,
    invoice_date        DATE,
    due_date            DATE,
    amount              NUMERIC(12,2),
    paid_date           DATE,
    paid_amount         NUMERIC(12,2),
    assigned_to         TEXT,       -- initials of billing staff (e.g. 'NK')
    notes               TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (intake_id, billing_phase_code)
);

-- Index for Natalie's queue view (filter by assigned_to + status)
CREATE INDEX IF NOT EXISTS idx_billing_queue_assigned_status
    ON billing_queue (assigned_to, status);

CREATE INDEX IF NOT EXISTS idx_billing_queue_intake
    ON billing_queue (intake_id);
