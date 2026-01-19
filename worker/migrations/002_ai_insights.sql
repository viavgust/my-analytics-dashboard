-- AI-generated insights for the dashboard
CREATE TABLE IF NOT EXISTS ai_insights (
  id TEXT PRIMARY KEY,
  created_at TEXT DEFAULT (datetime('now')),
  run_date TEXT NOT NULL, -- YYYY-MM-DD
  source TEXT NOT NULL,   -- ebay | telegram | youtube | calendar
  type TEXT NOT NULL,     -- money | margin | action | signal | plan
  period TEXT NOT NULL,   -- 7d | 30d | 90d | 180d | today | week
  title TEXT NOT NULL,
  text TEXT NOT NULL,
  actions_json TEXT,
  input_digest TEXT
);

CREATE INDEX IF NOT EXISTS idx_ai_insights_run_date ON ai_insights (run_date);
