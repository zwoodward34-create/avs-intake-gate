-- =============================================================
-- AVS Intake Gate — Wave 1 Migration
-- Run once in Supabase SQL editor (Dashboard → SQL Editor → New query)
-- =============================================================

-- 1. Flexible phase selection per project
--    Array of phase codes the project will actually go through.
--    e.g. '["SD","90%","IFP","CA"]'
ALTER TABLE intakes ADD COLUMN IF NOT EXISTS selected_phases JSONB DEFAULT NULL;

-- 2. Per-phase due dates
--    Object mapping phase code → ISO date string.
--    e.g. '{"SD":"2026-06-01","90%":"2026-07-15","IFP":"2026-08-01","CA":"2026-12-01"}'
ALTER TABLE intakes ADD COLUMN IF NOT EXISTS phase_due_dates JSONB DEFAULT NULL;

-- 3. CAD vs. Revit
--    Which software this project is being drafted in.
ALTER TABLE intakes ADD COLUMN IF NOT EXISTS cad_or_revit TEXT DEFAULT NULL;

-- 4. Human-written project overview / summary note
--    Short plain-English description of what the project actually is.
ALTER TABLE intakes ADD COLUMN IF NOT EXISTS project_overview TEXT DEFAULT NULL;

-- Verify
SELECT
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_name='intakes' AND column_name='selected_phases')   AS selected_phases_ok,
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_name='intakes' AND column_name='phase_due_dates')   AS phase_due_dates_ok,
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_name='intakes' AND column_name='cad_or_revit')      AS cad_or_revit_ok,
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_name='intakes' AND column_name='project_overview')  AS project_overview_ok;
