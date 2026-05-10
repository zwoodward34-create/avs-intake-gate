-- Migration 007: Proposal Pipeline & Follow-Up Staleness Tracking
-- Run in Supabase SQL Editor

-- 1. Add bid-tracking columns to intakes
ALTER TABLE intakes
  ADD COLUMN IF NOT EXISTS proposal_sent_date  TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS follow_up_count      INT DEFAULT 0,
  ADD COLUMN IF NOT EXISTS win_probability      INT DEFAULT 50;
