-- YouTube: ежедневные агрегаты
CREATE TABLE IF NOT EXISTS youtube_daily (
  updated_at TEXT PRIMARY KEY,                  -- ISO datetime
  views_today INTEGER,
  views_7d INTEGER,
  views_30d INTEGER,
  views_all_time INTEGER,
  subscribers INTEGER,
  new_videos_30d INTEGER,
  created_at TEXT DEFAULT (datetime('now'))
);

-- Продажи / eBay: дневные агрегаты
CREATE TABLE IF NOT EXISTS sales_daily (
  date TEXT PRIMARY KEY,                       -- YYYY-MM-DD
  total_sales INTEGER NOT NULL DEFAULT 0,      -- количество продаж/заказов
  total_revenue_cents INTEGER NOT NULL DEFAULT 0,
  total_profit_cents INTEGER NOT NULL DEFAULT 0,
  avg_profit_cents INTEGER NOT NULL DEFAULT 0,
  created_at TEXT DEFAULT (datetime('now'))
);

-- Последние посты Telegram
CREATE TABLE IF NOT EXISTS telegram_posts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  channel_slug TEXT NOT NULL,
  message_id TEXT NOT NULL UNIQUE,
  text TEXT NOT NULL,
  published_at TEXT NOT NULL,                  -- ISO timestamp
  message_url TEXT,
  created_at TEXT DEFAULT (datetime('now'))
);
