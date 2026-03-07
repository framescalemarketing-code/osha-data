CREATE TABLE IF NOT EXISTS raw_feed_items (
  id TEXT PRIMARY KEY,
  feed_name TEXT NOT NULL,
  feed_url TEXT NOT NULL,
  item_title TEXT NOT NULL,
  item_summary TEXT NOT NULL,
  item_url TEXT NOT NULL UNIQUE,
  published_at TEXT,
  raw_payload TEXT NOT NULL,
  processed_at TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
  id TEXT PRIMARY KEY,
  raw_item_id TEXT,
  company_name TEXT NOT NULL,
  company_name_normalized TEXT NOT NULL,
  industry TEXT NOT NULL,
  event_type TEXT NOT NULL,
  headline TEXT NOT NULL,
  summary TEXT NOT NULL,
  source_name TEXT NOT NULL,
  source_url TEXT NOT NULL,
  published_at TEXT,
  location TEXT,
  event_score REAL NOT NULL,
  signal_strength TEXT NOT NULL,
  review_status TEXT NOT NULL,
  raw_keywords TEXT NOT NULL,
  scale_clues TEXT NOT NULL,
  classification_reason TEXT NOT NULL,
  raw_payload TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_review_score
  ON events (review_status, event_score DESC, published_at DESC);

CREATE INDEX IF NOT EXISTS idx_events_company
  ON events (company_name_normalized, published_at DESC);

CREATE TABLE IF NOT EXISTS contacts (
  id TEXT PRIMARY KEY,
  company_name TEXT NOT NULL,
  company_name_normalized TEXT NOT NULL,
  contact_type TEXT NOT NULL,
  contact_value TEXT NOT NULL,
  name TEXT,
  title TEXT,
  source_name TEXT NOT NULL,
  source_url TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE (company_name_normalized, contact_type, contact_value)
);

CREATE INDEX IF NOT EXISTS idx_contacts_company
  ON contacts (company_name_normalized, contact_type);

CREATE TABLE IF NOT EXISTS event_contacts (
  event_id TEXT NOT NULL,
  contact_id TEXT NOT NULL,
  PRIMARY KEY (event_id, contact_id)
);

CREATE TABLE IF NOT EXISTS current_pipeline_signals (
  id TEXT PRIMARY KEY,
  company_name TEXT NOT NULL,
  company_name_normalized TEXT NOT NULL,
  region TEXT,
  site_address TEXT,
  site_city TEXT,
  site_state TEXT,
  site_zip TEXT,
  industry_segment TEXT,
  current_priority TEXT,
  current_action TEXT,
  overall_sales_score REAL NOT NULL,
  matched_sources TEXT,
  reason_to_contact TEXT,
  reason_to_call_now TEXT,
  why_fit TEXT,
  why_now TEXT,
  raw_payload TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_current_pipeline_company
  ON current_pipeline_signals (company_name_normalized, overall_sales_score DESC);

CREATE TABLE IF NOT EXISTS intersection_alerts (
  id TEXT PRIMARY KEY,
  company_name TEXT NOT NULL,
  company_name_normalized TEXT NOT NULL,
  current_signal_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  alert_score REAL NOT NULL,
  alert_reason TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE (current_signal_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_intersection_score
  ON intersection_alerts (alert_score DESC, updated_at DESC);
