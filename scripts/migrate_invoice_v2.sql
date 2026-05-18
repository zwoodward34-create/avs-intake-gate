-- =============================================================
-- AVS Intake Gate — Invoice V2 Migration
-- Run once in Supabase SQL editor (Dashboard → SQL Editor → New query)
-- =============================================================

-- 1. Client Profiles
--    Stores per-client invoicing requirements so the system can
--    pre-populate and validate invoices automatically.
CREATE TABLE IF NOT EXISTS client_profiles (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    client_name     TEXT        NOT NULL,
    requires_po     BOOLEAN     NOT NULL DEFAULT FALSE,
    po_number       TEXT        NOT NULL DEFAULT '',
    custom_invoice_fields  JSONB NOT NULL DEFAULT '[]'::jsonb,
    invoice_notes   TEXT        NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT client_profiles_client_name_key UNIQUE (client_name)
);

-- 2. Extend invoices table
ALTER TABLE invoices ADD COLUMN IF NOT EXISTS po_number           TEXT    DEFAULT '';
ALTER TABLE invoices ADD COLUMN IF NOT EXISTS po_attachment_url   TEXT    DEFAULT '';
ALTER TABLE invoices ADD COLUMN IF NOT EXISTS custom_fields       JSONB   DEFAULT '[]'::jsonb;
ALTER TABLE invoices ADD COLUMN IF NOT EXISTS use_timesheet_hours BOOLEAN DEFAULT FALSE;
ALTER TABLE invoices ADD COLUMN IF NOT EXISTS invoice_date        DATE;

-- 3. Extend project_expenses table
--    Separates "reimbursed to employee" from "invoiced to client"
ALTER TABLE project_expenses ADD COLUMN IF NOT EXISTS reimbursed_at          TIMESTAMPTZ;
ALTER TABLE project_expenses ADD COLUMN IF NOT EXISTS reimbursed_by          TEXT;
ALTER TABLE project_expenses ADD COLUMN IF NOT EXISTS client_invoiced_at     TIMESTAMPTZ;
ALTER TABLE project_expenses ADD COLUMN IF NOT EXISTS client_invoice_number  TEXT;

-- Allow 'reimbursed' as a valid status alongside existing values
-- (no enum constraint exists by default in Supabase / PostgreSQL TEXT columns)

-- Verify
SELECT 'client_profiles OK' AS status WHERE EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_name = 'client_profiles'
);
