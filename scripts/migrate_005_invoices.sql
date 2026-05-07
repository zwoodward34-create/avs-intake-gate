-- Migration 005: Invoice system + intake billing columns
-- Run in Supabase SQL Editor

-- 1. Add billing columns to intakes (safe if already exist)
ALTER TABLE intakes
  ADD COLUMN IF NOT EXISTS total_contract_fee  NUMERIC(10,2),
  ADD COLUMN IF NOT EXISTS is_retainer_paid    BOOLEAN DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS last_invoiced_at    TIMESTAMPTZ;

-- 2. Create invoices table
CREATE TABLE IF NOT EXISTS invoices (
    id             BIGSERIAL PRIMARY KEY,
    invoice_number TEXT UNIQUE NOT NULL,
    intake_id      BIGINT NOT NULL,
    phase_code     TEXT NOT NULL,
    amount         NUMERIC(10,2) NOT NULL,
    status         TEXT NOT NULL DEFAULT 'draft',  -- draft | sent | paid
    created_by     TEXT,
    notes          TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at        TIMESTAMPTZ,
    paid_at        TIMESTAMPTZ
);

ALTER TABLE invoices ENABLE ROW LEVEL SECURITY;

CREATE POLICY "service role full access" ON invoices
    FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- 3. View: phases approved by Mo but no formal invoice yet
CREATE OR REPLACE VIEW ready_to_bill AS
SELECT
    pbp.intake_id,
    pbp.billing_phase_code,
    pbp.fee_amount,
    pbp.fee_pct,
    pbp.invoice_fee_override,
    pbp.invoice_approved_at,
    pbp.invoice_approved_by,
    i.project_number,
    i.project_name,
    i.client_name,
    i.location_region,
    i.total_contract_fee,
    i.is_retainer_paid
FROM project_billing_phases pbp
JOIN intakes i ON i.id = pbp.intake_id
WHERE pbp.status = 'invoice_approved'
  AND NOT EXISTS (
      SELECT 1 FROM invoices inv
      WHERE inv.intake_id   = pbp.intake_id
        AND inv.phase_code  = pbp.billing_phase_code
  )
ORDER BY pbp.invoice_approved_at DESC;
