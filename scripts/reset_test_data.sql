-- ── AVS Test Data Reset ──────────────────────────────────────────────────────
-- Removes all intake / project / billing / calendar data entered during testing.
--
-- PRESERVED (not touched):
--   profiles              — user accounts and roles
--   billing_phase_definitions — billing config (Retainer, SD, DD, CD, CA)
--   templates             — proposal/document templates
--
-- CLEARED:
--   intakes + all dependent records (calendar events, budgets, billing phases,
--   time entries, invoices, phase history, billing queue)
--   historical_projects   — projects auto-saved via mark-won
--   timesheet_submissions — submitted timesheets
--
-- OPTIONAL (see bottom):
--   time_off              — engineer PTO entries (commented out by default)
--   project_number_sequence — reset to 9000
--
-- Run in: Supabase Dashboard → SQL Editor → New query
-- ─────────────────────────────────────────────────────────────────────────────

BEGIN;

-- 1. Dependent tables first (FK constraints — delete children before parents)

-- Invoice records tied to intakes
DELETE FROM invoices;

-- Phase advancement log tied to intakes
DELETE FROM project_phase_history;

-- Natalie's billing queue (also has ON DELETE CASCADE from intakes, but explicit is safer)
DELETE FROM billing_queue;

-- Per-phase billing breakdown tied to intakes
DELETE FROM project_billing_phases;

-- Budgeted-hours allocation per phase tied to intakes
DELETE FROM phase_budgets;

-- All time log entries (linked to intakes via intake_id)
DELETE FROM time_entries;

-- Timesheet submission records (standalone — not linked by FK, but test data)
DELETE FROM timesheet_submissions;

-- Calendar events (linked to intakes via intake_id)
DELETE FROM calendar_events;

-- 2. Historical projects database (populated by mark-won during testing)
DELETE FROM historical_projects;

-- 3. Main intakes table — all intake/proposal/project records
DELETE FROM intakes;

-- 4. Reset project number counter back to 9000
--    (so the next real project starts at 9001, matching your numbering convention)
UPDATE project_number_sequence
SET    last_number = 9000,
       updated_at  = NOW()
WHERE  id = 1;


-- ── Optional blocks — uncomment if you also want to clear these ──────────────

-- Clear engineer PTO entries (keep commented if PTO records are real):
-- DELETE FROM time_off;

-- ─────────────────────────────────────────────────────────────────────────────
COMMIT;
