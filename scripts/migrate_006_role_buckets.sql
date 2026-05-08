-- Migration 006: Role-Based Hour Bucketing
-- Run in Supabase SQL Editor

-- 1. Add role_type to time_entries (senior | production)
ALTER TABLE time_entries
  ADD COLUMN IF NOT EXISTS role_type text;

-- 2. Backfill role_type from engineer_initials
UPDATE time_entries
SET role_type = CASE
  WHEN engineer_initials IN ('MK', 'NK', 'RO', 'JW', 'JR', 'RK') THEN 'senior'
  WHEN engineer_initials IN ('RS', 'SW', 'JP')                    THEN 'production'
  ELSE 'senior'
END
WHERE role_type IS NULL;

-- 3. Add bucket_allocation to phase_budgets (jsonb: {senior, production})
ALTER TABLE phase_budgets
  ADD COLUMN IF NOT EXISTS bucket_allocation jsonb;

-- 4. Backfill bucket_allocation for existing phase budget rows
UPDATE phase_budgets
SET bucket_allocation = jsonb_build_object(
  'senior',     ROUND((budgeted_hours * 0.40)::numeric, 2),
  'production', ROUND((budgeted_hours * 0.60)::numeric, 2)
)
WHERE bucket_allocation IS NULL;
