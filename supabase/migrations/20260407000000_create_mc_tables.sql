-- Mission Control: Supabase Migration
-- Creates mc_agents, mc_tasks, mc_task_events tables

-- Helper function for DDL execution (used by migration scripts)
CREATE OR REPLACE FUNCTION exec_ddl(sql text) RETURNS void AS $$
BEGIN
  EXECUTE sql;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- MC Agents
CREATE TABLE IF NOT EXISTS mc_agents (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  company TEXT,
  type TEXT DEFAULT 'loop',
  description TEXT,
  capabilities JSONB DEFAULT '[]'::jsonb,
  current_task_id TEXT,
  current_task JSONB,
  error TEXT,
  last_action TEXT,
  last_heartbeat TIMESTAMPTZ,
  status TEXT DEFAULT 'idle',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- MC Tasks
CREATE TABLE IF NOT EXISTS mc_tasks (
  id TEXT PRIMARY KEY,
  company TEXT NOT NULL,
  category TEXT NOT NULL,
  title TEXT NOT NULL,
  detail TEXT DEFAULT '',
  owner TEXT DEFAULT 'Bot',
  "trigger" TEXT DEFAULT 'Agent',
  status TEXT DEFAULT 'queued',
  priority TEXT,
  severity TEXT,
  estimated_value NUMERIC,
  tier TEXT,
  impact TEXT,
  source_url TEXT,
  due_date TIMESTAMPTZ,
  assignee TEXT,
  agent TEXT,
  current_step TEXT,
  next_action TEXT,
  next_action_at TIMESTAMPTZ,
  last_action_at TIMESTAMPTZ,
  error TEXT,
  event_type TEXT,
  event_data JSONB,
  sla TEXT,
  sla_deadline TIMESTAMPTZ,
  sla_status TEXT,
  time_left TEXT,
  sla_overdue_at TIMESTAMPTZ,
  sla_escalated_at TIMESTAMPTZ,
  bot_blocked BOOLEAN DEFAULT FALSE,
  rolled_forward BOOLEAN DEFAULT FALSE,
  original_priority TEXT,
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- MC Task Events (audit trail)
CREATE TABLE IF NOT EXISTS mc_task_events (
  id BIGSERIAL PRIMARY KEY,
  task_id TEXT REFERENCES mc_tasks(id) ON DELETE CASCADE,
  event_type TEXT NOT NULL,
  detail TEXT,
  agent TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_mc_tasks_status ON mc_tasks(status);
CREATE INDEX IF NOT EXISTS idx_mc_tasks_company ON mc_tasks(company);
CREATE INDEX IF NOT EXISTS idx_mc_tasks_created ON mc_tasks(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_mc_agents_name ON mc_agents(name);
CREATE INDEX IF NOT EXISTS idx_mc_task_events_task ON mc_task_events(task_id);
