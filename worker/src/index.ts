// @ts-nocheck
// Тип окружения: сюда Cloudflare "привяжет" нашу D1-базу с binding = "DB"
export interface Env {
  DB: D1Database;
  TELEGRAM_CHANNEL_SLUG?: string;
  GEMINI_API_KEY?: string;
  COMPOSIO_API_KEY?: string;
  COMPOSIO_YT_AUTH_CONFIG_ID?: string;
  COMPOSIO_YT_ACCOUNT_ID?: string;
  COMPOSIO_YT_CHANNEL_ID?: string;
  COMPOSIO_YT_ENTITY_ID?: string;
  COMPOSIO_YT_CHANNEL_HANDLE?: string;
  YOUTUBE_SOURCE_HANDLE?: string;
  YOUTUBE_SOURCE_CHANNEL_ID?: string;
  COMPOSIO_SHEETS_ACCOUNT_ID?: string;
  COMPOSIO_SHEETS_ENTITY_ID?: string;
  SALES_SHEET_ID?: string;
  SALES_SHEET_RANGE?: string;
  COMPOSIO_CALENDAR_ACCOUNT_ID?: string;
  COMPOSIO_CALENDAR_ENTITY_ID?: string;
  DEMO_MODE?: string;
  INSIGHTS_LANG?: string;
}

type YoutubeMetrics = {
  viewsToday: number;
  views7d: number;
  views30d: number;
  allTimeViews: number;
  subscribers: number;
  newVideos30d: number;
  videoCount: number;
  topVideo?: {
    title: string;
    views: number;
    publishedAt?: string;
    url?: string;
    videoId?: string;
  } | null;
  latestVideos?: {
    title: string;
    url: string;
    publishedAt: string;
    thumbnailUrl?: string | null;
    videoId?: string | null;
  }[];
};

const TOP_VIDEO_ID = "_AMRlYI3Q-o";
const TELEGRAM_RETENTION_DAYS = 14;
const TELEGRAM_MAX_POSTS = 30;
const TELEGRAM_PARSE_LIMIT = 24;
const INSIGHTS_MIN = 6;
const INSIGHTS_MAX = 8;
const INSIGHTS_MIN_ACTION = 2;
const INSIGHTS_MIN_EBAY = 5;
const INSIGHTS_MAX_EBAY = 5;
const INSIGHTS_MONEY_TARGET = 2;
const DEFAULT_INSIGHTS_LANG = "ru";
const INSIGHTS_MIN_TELEGRAM = 1;
const INSIGHTS_MAX_TELEGRAM = 2;
const INSIGHTS_MAX_ACTIONS = 3;
const YT_CACHE_TTL_DAYS = 7;
const MAX_TITLE_LENGTH = 60;
const MAX_TEXT_LENGTH = 300;
const MAX_SUMMARY_TEXT_LENGTH = 600;
const SUMMARY_TIMEZONE = "America/New_York";
const SUMMARY_LOOKAHEAD_DAYS = 14;
const LESSON_TITLE_RE = /(урок|занятие|курс|нейросет|homework|lesson)/i;

// Обновление всех источников (YouTube / Sheets / Telegram / Calendar) параллельно с таймаутами
async function refreshAll(env: Env) {
  const demoMode = env.DEMO_MODE === "true";
  await Promise.allSettled([
    withTimeout(
      (async () => {
        const ytMetrics = await fetchYoutubeMetricsFromComposio(env);
        if (ytMetrics) {
          await upsertYoutubeSnapshot(env, ytMetrics);
        } else {
          console.warn("YouTube refresh skipped (Composio failed). Keeping last known metrics. DEMO_MODE=", demoMode);
          if (demoMode) {
            await refreshYoutubeDemo(env);
          }
        }
      })(),
      10000,
      "youtube"
    ),
    withTimeout(refreshSalesFromSheets(env), 10000, "sales"),
    withTimeout(refreshTelegram(env), 8000, "telegram"),
    withTimeout(refreshCalendar(env), 8000, "calendar"),
  ]);
}

// Главный обработчик Worker-а
export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    try {
      const url = new URL(request.url);

  // CORS preflight
  if (request.method === "OPTIONS") {
    const corsPaths = ["/api/dashboard", "/api/refresh", "/api/insights/latest", "/api/insights/generate"];
    if (corsPaths.includes(url.pathname)) {
      return new Response(null, {
        status: 204,
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type",
        },
      });
    }
  }

      // GET /api/dashboard — отдать данные для дашборда
      if (request.method === "GET" && url.pathname === "/api/dashboard") {
        return handleDashboard(env);
      }

      if (request.method === "GET" && url.pathname === "/api/insights/latest") {
        return handleInsightsLatest(env);
      }

      if (request.method === "POST" && url.pathname === "/api/insights/generate") {
        return handleInsightsGenerate(env);
      }

      // POST /api/refresh — пока заглушка
      if ((request.method === "POST" || request.method === "GET") && url.pathname === "/api/refresh") {
        console.log("Refresh called at", new Date().toISOString());
        console.log("Has COMPOSIO_API_KEY:", !!env.COMPOSIO_API_KEY);
        await refreshAll(env);
        return jsonResponse({
          ok: true,
          message: "Refresh completed (telegram/youtube/sales/calendar)",
        });
      }

      // Всё остальное — 404
      return new Response("Not found", { status: 404 });
    } catch (err: any) {
      console.error("Unhandled fetch error", err);
      return jsonResponse(
        {
          error: "Unhandled",
          message: String(err?.message || err),
          stack: err?.stack,
        },
        500
      );
    }
  },
};

// Cron-хук Cloudflare: запускает тот же refresh раз в день (см. crons в wrangler.toml)
export const scheduled = async (_event: ScheduledEvent, env: Env, _ctx: ExecutionContext) => {
  console.log("Scheduled refresh at", new Date().toISOString());
  await refreshAll(env);
  await generateAndStoreInsights(env, "cron");
};

// Обработчик /api/dashboard
async function handleDashboard(env: Env): Promise<Response> {
  try {
    const channelSlug = env.TELEGRAM_CHANNEL_SLUG || "my_channel";
    const demoPayload = buildDemoDashboardPayload();

    // YouTube: берём последнюю строку агрегатов
    let ytRow:
      | {
          updated_at: string;
          views_today: number;
          views_7d: number;
          views_30d: number;
          views_all_time: number;
          subscribers: number;
          new_videos_30d: number;
          videos_total?: number | null;
          top_video_title?: string | null;
          top_video_views?: number | null;
          top_video_url?: string | null;
          top_video_published_at?: string | null;
        }
      | null = null;

    try {
      ytRow = await env.DB.prepare(
        `SELECT
           updated_at,
           views_today,
           views_7d,
           views_30d,
           views_all_time,
           subscribers,
           new_videos_30d,
           videos_total,
           top_video_title,
           top_video_views,
           top_video_url,
           top_video_published_at
         FROM youtube_daily
         ORDER BY updated_at DESC
         LIMIT 1`
      ).first();
    } catch (err) {
      // если нет колонки videos_total — добавим и попробуем снова
      try {
        await env.DB.prepare("ALTER TABLE youtube_daily ADD COLUMN videos_total INTEGER").run();
        await env.DB.prepare("ALTER TABLE youtube_daily ADD COLUMN top_video_title TEXT").run();
        await env.DB.prepare("ALTER TABLE youtube_daily ADD COLUMN top_video_views INTEGER").run();
        await env.DB.prepare("ALTER TABLE youtube_daily ADD COLUMN top_video_url TEXT").run();
        await env.DB.prepare("ALTER TABLE youtube_daily ADD COLUMN top_video_published_at TEXT").run();
        ytRow = await env.DB.prepare(
          `SELECT
             updated_at,
             views_today,
             views_7d,
             views_30d,
             views_all_time,
             subscribers,
             new_videos_30d,
             videos_total,
             top_video_title,
             top_video_views,
             top_video_url,
             top_video_published_at
           FROM youtube_daily
           ORDER BY updated_at DESC
           LIMIT 1`
        ).first();
      } catch (_) {
        ytRow = await env.DB.prepare(
          `SELECT
             updated_at,
             views_today,
             views_7d,
             views_30d,
             views_all_time,
             subscribers,
             new_videos_30d
           FROM youtube_daily
           ORDER BY updated_at DESC
           LIMIT 1`
        ).first();
      }
    }

    const salesResult = await env.DB.prepare(
      "SELECT * FROM sales_daily ORDER BY date DESC"
    ).all();

    const telegramResult = await env.DB.prepare(
      "SELECT channel_slug, message_id, text, published_at, message_url FROM telegram_posts WHERE channel_slug = ? ORDER BY published_at DESC LIMIT 3"
    )
      .bind(channelSlug)
      .all();

    const salesRows = salesResult.results ?? [];
    const telegramRows = telegramResult.results ?? [];

    // Если в базе вообще нет данных — отдаём демо-пейлоад
    if (!ytRow && !salesRows.length && !telegramRows.length) {
      return jsonResponse(demoPayload);
    }

    const updatedAt = new Date().toISOString();

    const latestVideos = await fetchLatestYoutubeVideos(env);

    // --- YouTube метрики и график ---
    let youtubeMetrics;
    let youtubeChart;
    let youtubeTopVideo: YoutubeMetrics["topVideo"] | null | undefined;

    if (ytRow) {
      youtubeMetrics = {
        viewsToday: ytRow.views_today ?? 0,
        views7d: ytRow.views_7d ?? 0,
        views30d: ytRow.views_30d ?? 0,
        allTimeViews: ytRow.views_all_time ?? 0,
        newVideos30d: ytRow.new_videos_30d ?? 0,
        subscribers: ytRow.subscribers ?? 0,
      };
      youtubeTopVideo =
        ytRow.top_video_title && ytRow.top_video_views !== undefined
          ? {
              title: ytRow.top_video_title,
              views: ytRow.top_video_views ?? 0,
              publishedAt: ytRow.top_video_published_at ?? undefined,
              url: ytRow.top_video_url ?? undefined,
            }
          : null;
      youtubeChart = {
        granularity: "month",
        points: [],
      };
    } else {
      youtubeMetrics = {
        viewsToday: 0,
        views7d: 0,
        views30d: 0,
        allTimeViews: 0,
        newVideos30d: 0,
        subscribers: 0,
      };
      youtubeChart = {
        granularity: "month",
        points: [],
      };
      youtubeTopVideo = null;
    }
    const youtubeLatestVideos = latestVideos.length > 0 ? latestVideos : [];

    // --- Sales / eBay метрики и график ---

    // Вспомогательная функция: сумма по полю
    const sumField = (rows: any[], field: string): number =>
      rows.reduce((sum, row) => sum + (row[field] ?? 0), 0);

    let salesMetrics;
    let salesChart;

    if (salesRows.length) {
      const totalSalesAll = sumField(salesRows, "total_sales");
      const totalRevenueAllCents = sumField(salesRows, "total_revenue_cents");
      const totalProfitAllCents = sumField(salesRows, "total_profit_cents");
      const avgProfitAll =
        totalSalesAll > 0 ? centsToDollars(totalProfitAllCents) / totalSalesAll : 0;

      salesMetrics = {
        totalSales: totalSalesAll,
        totalRevenue: centsToDollars(totalRevenueAllCents),
        totalProfit: centsToDollars(totalProfitAllCents),
        avgProfit: avgProfitAll,
      };

      const salesPoints = salesRows
        .slice()
        .reverse()
        .map((row: any) => ({
          label: row.date,
          revenue: centsToDollars(row.total_revenue_cents ?? 0),
        }));

      // ограничим график последними 120 точками (если нужно)
      const limitedPoints = salesPoints.slice(-120);

      salesChart = {
        granularity: "day",
        points: limitedPoints,
      };
    } else {
      salesMetrics = {
        totalSales: 0,
        totalRevenue: 0,
        totalProfit: 0,
        avgProfit: 0,
      };
      salesChart = {
        granularity: "day",
        points: [],
      };
    }

    // --- Telegram ---

    const telegramChannel =
      (telegramRows[0] as any)?.channel_slug ?? channelSlug;

    const telegramPosts =
      telegramRows.length > 0
        ? telegramRows.map((row: any) => ({
            messageId: row.message_id,
            text: row.text,
            publishedAt: row.published_at,
            url: row.message_url ?? null,
          }))
        : demoPayload.telegram.posts;

    const payload = {
      updatedAt,
      telegram: {
        channel: telegramChannel,
        posts: telegramPosts,
      },
      youtube: {
        metrics: youtubeMetrics,
        chart: youtubeChart,
        topVideo: youtubeTopVideo,
        latestVideos: youtubeLatestVideos,
      },
      sales: {
        metrics: salesMetrics,
        chart: salesChart,
      },
      calendar: await loadCalendarEvents(env),
    };

    return jsonResponse(payload);
  } catch (err) {
    console.error("Error in /api/dashboard:", err);
    // На всякий случай, если что-то упало — отдаём демо-данные
    return jsonResponse(buildDemoDashboardPayload());
  }
}

// Демо-пейлоад, который всегда подходит под UI
function buildDemoDashboardPayload() {
  return {
    updatedAt: new Date().toISOString(),
    telegram: {
      channel: "greekpod101",
      posts: [
        {
          messageId: "451",
          text: "5 must-know Greek words на этой неделе — проверь список",
          publishedAt: "2025-03-15T07:10:00Z",
        },
        {
          messageId: "448",
          text: "Советы по греческому произношению — разбор ошибок",
          publishedAt: "2025-03-14T12:05:00Z",
        },
        {
          messageId: "443",
          text: "Вопросы подписчиков про греческие артикли — с примерами",
          publishedAt: "2025-03-12T18:45:00Z",
        },
      ],
    },
    youtube: {
      metrics: {
        viewsToday: 120,
        views7d: 860,
        views30d: 3200,
        allTimeViews: 42000,
        newVideos30d: 4,
        subscribers: 12000,
      },
      chart: {
        granularity: "month",
      points: [
          { label: "Oct", views: 1200 },
          { label: "Nov", views: 1800 },
          { label: "Dec", views: 1500 },
          { label: "Jan", views: 2200 },
          { label: "Feb", views: 2800 },
          { label: "Mar", views: 3240 },
        ],
      },
      latestVideos: [
        {
          title: "5 Must-Know Greek Words: Quality and Distance",
          url: "https://www.youtube.com/watch?v=abc123greek",
          publishedAt: "2025-12-18T12:23:41+00:00",
          thumbnailUrl: "https://i1.ytimg.com/vi/abc123greek/hqdefault.jpg",
          videoId: "abc123greek",
        },
        {
          title: "How is Your Greek? Talking about Your Greek Language Skills",
          url: "https://www.youtube.com/watch?v=def456greek",
          publishedAt: "2025-12-16T16:52:50+00:00",
          thumbnailUrl: "https://i1.ytimg.com/vi/def456greek/hqdefault.jpg",
          videoId: "def456greek",
        },
        {
          title: "Learn Greek Anywhere, Anytime on Your Mobile",
          url: "https://www.youtube.com/watch?v=ghi789greek",
          publishedAt: "2025-12-14T13:53:26+00:00",
          thumbnailUrl: "https://i3.ytimg.com/vi/ghi789greek/hqdefault.jpg",
          videoId: "ghi789greek",
        },
      ],
    },
    sales: {
      metrics: {
        totalSales: 147,
        totalRevenue: 3680.0,
        totalProfit: 1240.0,
        avgProfit: 8.44,
      },
      chart: {
        granularity: "month",
        points: [
          { label: "Oct", revenue: 320 },
          { label: "Nov", revenue: 480 },
          { label: "Dec", revenue: 720 },
          { label: "Jan", revenue: 580 },
          { label: "Feb", revenue: 890 },
          { label: "Mar", revenue: 710 },
        ],
      },
    },
  };
}

// Утилита для JSON-ответа
function jsonResponse(data: unknown, status: number = 200, cache: "default" | "no-store" = "default"): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
      ...(cache === "no-store" ? { "Cache-Control": "no-store" } : {}),
    },
  });
}

type InsightCard = {
  id: string;
  createdAt: string;
  runDate: string;
  source: "summary" | "ebay" | "telegram" | "youtube" | "calendar";
  type: "money" | "margin" | "action" | "signal" | "plan" | "recommendation";
  period: "7d" | "30d" | "90d" | "180d" | "today" | "week" | "3d";
  title: string;
  text: string;
  actions: string[];
  inputDigest?: string | null;
};

function getRunDate(date = new Date(), timeZone = "America/New_York"): string {
  // Формируем дату в нужном часовом поясе, чтобы дневной прогон совпадал с локальным днём
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(date);
  const lookup = Object.fromEntries(parts.map((p) => [p.type, p.value]));
  return `${lookup.year}-${lookup.month}-${lookup.day}`;
}

async function ensureAiInsightsTable(env: Env): Promise<boolean> {
  try {
    await env.DB.prepare(
      `CREATE TABLE IF NOT EXISTS ai_insights (
         id TEXT PRIMARY KEY,
         created_at TEXT DEFAULT (datetime('now')),
         run_date TEXT NOT NULL,
         source TEXT NOT NULL,
         type TEXT NOT NULL,
         period TEXT NOT NULL,
         title TEXT NOT NULL,
         text TEXT NOT NULL,
         actions_json TEXT,
         input_digest TEXT
       )`
    ).run();
    await env.DB.prepare(
      `CREATE INDEX IF NOT EXISTS idx_ai_insights_run_date ON ai_insights (run_date)`
    ).run();
    return true;
  } catch (err) {
    console.error("ensureAiInsightsTable failed", err);
    return false;
  }
}

function parseActions(actionsJson: string | null | undefined): string[] {
  if (!actionsJson) return [];
  try {
    const parsed = JSON.parse(actionsJson);
    if (Array.isArray(parsed)) {
      return parsed
        .map((a) => (typeof a === "string" ? a.trim() : ""))
        .filter(Boolean)
        .slice(0, 6);
    }
    return [];
  } catch {
    return [];
  }
}

function stripMarkdownAndEmojis(input: string): string {
  const noMarkdownLinks = input.replace(/\[([^\]]+)\]\(([^)]+)\)/g, "$1");
  const noMarkdown = noMarkdownLinks.replace(/[*_`~>#]+/g, " ");
  const noHtmlTags = noMarkdown.replace(/<\/?[^>]+>/g, " ");
  const noEntities = decodeHtmlEntities(noHtmlTags)
    .replace(/&nbsp;/gi, " ")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/g, "'");
  const noEmojis = noEntities.replace(/\p{Extended_Pictographic}/gu, "");
  return noEmojis.replace(/\s+/g, " ").trim();
}

function sanitizeTextField(value: any, maxLen = MAX_TEXT_LENGTH): string {
  const text = typeof value === "string" ? value : "";
  if (!text) return "";
  const cleaned = stripMarkdownAndEmojis(text);
  if (cleaned.length <= maxLen) return cleaned;
  return `${cleaned.slice(0, Math.max(0, maxLen - 3))}...`;
}

function sanitizeMultilineText(value: any, maxLen = MAX_TEXT_LENGTH): string {
  const text = typeof value === "string" ? value.replace(/\r\n?/g, "\n") : "";
  if (!text) return "";
  const lines = text
    .split("\n")
    .map((line) => stripMarkdownAndEmojis(line).trim())
    .filter(Boolean);
  let joined = lines.join("\n");
  if (joined.length > maxLen) {
    joined = `${joined.slice(0, Math.max(0, maxLen - 3))}...`;
  }
  return joined;
}

function getDatePartsInTimeZone(date: Date, timeZone: string) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(date);
  const map: Record<string, string> = {};
  for (const part of parts) {
    if (part.type !== "literal") map[part.type] = part.value;
  }
  return {
    year: Number(map.year),
    month: Number(map.month),
    day: Number(map.day),
  };
}

function diffDaysInTimeZone(from: Date, to: Date, timeZone: string): number {
  const fromParts = getDatePartsInTimeZone(from, timeZone);
  const toParts = getDatePartsInTimeZone(to, timeZone);
  const fromUtc = Date.UTC(fromParts.year, fromParts.month - 1, fromParts.day);
  const toUtc = Date.UTC(toParts.year, toParts.month - 1, toParts.day);
  return Math.round((toUtc - fromUtc) / 86_400_000);
}

function formatDateTimeInTimeZone(date: Date, timeZone: string) {
  const datePart = new Intl.DateTimeFormat("ru-RU", {
    timeZone,
    day: "2-digit",
    month: "2-digit",
  }).format(date);
  const timePart = new Intl.DateTimeFormat("ru-RU", {
    timeZone,
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(date);
  return { date: datePart, time: timePart };
}

function buildStudyBlock(events: { title: string; start: string }[] | undefined | null): string {
  const fallback = "Учёба: следующий урок не найден в календаре (проверь события).\nДомашка: готова? (Да/Нет)";
  if (!events || events.length === 0) return fallback;
  const now = new Date();
  const candidates = events
    .map((ev) => {
      if (!ev?.start) return null;
      const title = ev.title || "";
      if (!LESSON_TITLE_RE.test(title)) return null;
      const startDate = new Date(ev.start);
      if (Number.isNaN(startDate.getTime())) return null;
      if (startDate < now) return null;
      const daysAway = diffDaysInTimeZone(now, startDate, SUMMARY_TIMEZONE);
      if (daysAway < 0 || daysAway > SUMMARY_LOOKAHEAD_DAYS) return null;
      return { date: startDate, daysAway };
    })
    .filter(Boolean) as { date: Date; daysAway: number }[];

  if (candidates.length === 0) return fallback;
  candidates.sort((a, b) => a.date.getTime() - b.date.getTime());
  const next = candidates[0];
  const formatted = formatDateTimeInTimeZone(next.date, SUMMARY_TIMEZONE);
  const dayPhrase =
    next.daysAway <= 0 ? "сегодня" : next.daysAway === 1 ? "через 1 день" : `через ${next.daysAway} дней`;
  return `Учёба: следующий урок — ${dayPhrase} (${formatted.date}, ${formatted.time})\nДомашка: готова? (Да/Нет)`;
}

function sanitizeTitleField(value: any): string {
  const title = sanitizeTextField(value, MAX_TITLE_LENGTH);
  return title || "Инсайт";
}

function sanitizeActionsField(actions: any): string[] {
  if (!Array.isArray(actions)) return [];
  const cleaned = actions
    .map((a) => sanitizeTextField(typeof a === "string" ? a : "", 120))
    .filter(Boolean);
  return cleaned.slice(0, INSIGHTS_MAX_ACTIONS);
}

function mapInsightRow(row: any): InsightCard {
  const source: InsightCard["source"] = row.source ?? "ebay";
  return {
    id: row.id,
    createdAt: row.created_at ?? row.createdAt ?? new Date().toISOString(),
    runDate: row.run_date ?? row.runDate ?? getRunDate(),
    source,
    type: row.type ?? "action",
    period: row.period ?? "7d",
    title: sanitizeTitleField(row.title ?? "Insight"),
    text:
      source === "summary"
        ? sanitizeMultilineText(row.text ?? "", MAX_SUMMARY_TEXT_LENGTH)
        : source === "telegram" || source === "youtube" || source === "calendar"
          ? sanitizeMultilineText(row.text ?? "", MAX_TEXT_LENGTH)
          : sanitizeTextField(row.text ?? "", MAX_TEXT_LENGTH),
    actions: sanitizeActionsField(Array.isArray(row.actions) ? row.actions : parseActions(row.actions_json)),
    inputDigest: row.input_digest ?? row.inputDigest ?? null,
  };
}

async function loadLatestInsights(env: Env): Promise<{ runDate: string | null; insights: InsightCard[] }> {
  const ok = await ensureAiInsightsTable(env);
  if (!ok) {
    return { runDate: null, insights: [] };
  }

  try {
    const latest = await env.DB.prepare("SELECT run_date FROM ai_insights ORDER BY run_date DESC LIMIT 1").first<{
      run_date: string;
    }>();
    if (!latest?.run_date) return { runDate: null, insights: [] };

    const { results } = await env.DB.prepare(
      "SELECT id, created_at, run_date, source, type, period, title, text, actions_json, input_digest FROM ai_insights WHERE run_date = ? ORDER BY created_at DESC"
    )
      .bind(latest.run_date)
      .all<any>();

    return {
      runDate: latest.run_date,
      insights: (results || []).map(mapInsightRow),
    };
  } catch (err) {
    console.error("loadLatestInsights failed", err);
    return { runDate: null, insights: [] };
  }
}

async function storeInsights(env: Env, runDate: string, insights: InsightCard[], inputDigest?: string | null) {
  const ok = await ensureAiInsightsTable(env);
  if (!ok) {
    console.warn("Insights table unavailable, skip persisting");
    return;
  }
  try {
    await env.DB.prepare("DELETE FROM ai_insights WHERE run_date = ?").bind(runDate).run();
    const insert = env.DB.prepare(
      "INSERT INTO ai_insights (id, run_date, source, type, period, title, text, actions_json, input_digest) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)"
    );
    for (const card of insights) {
      await insert
        .bind(
          card.id,
          runDate,
          card.source,
          card.type,
          card.period,
          card.title,
          card.text,
          card.actions?.length ? JSON.stringify(card.actions.slice(0, 6)) : null,
          inputDigest ?? card.inputDigest ?? null
        )
        .run();
    }
  } catch (err) {
    console.error("storeInsights failed", err);
  }
}

async function buildInputDigest(payload: unknown): Promise<string> {
  const encoder = new TextEncoder();
  const data = encoder.encode(JSON.stringify(payload));
  const hashBuffer = await crypto.subtle.digest("SHA-256", data);
  return Array.from(new Uint8Array(hashBuffer))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

// Переводим центы в доллары с двумя знаками после запятой
function centsToDollars(cents: number | null | undefined): number {
  const value = typeof cents === "number" ? cents : 0;
  return Math.round(value) / 100;
}

function moneyStringToCents(input: any): number {
  if (typeof input !== "string") return 0;
  const cleaned = input.replace(/,/g, ".").replace(/\s+/g, "").replace(/[^0-9.\-]/g, "");
  const n = Number(cleaned);
  if (!Number.isFinite(n)) return 0;
  return Math.round(n * 100);
}

function parseNumber(value: any, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

type SalesDailyRow = {
  date: string;
  total_sales: number;
  total_revenue_cents: number;
  total_profit_cents: number;
  avg_profit_cents: number;
};

type SalesSummary = {
  last7Revenue: number;
  prev7Revenue: number;
  last7Profit: number;
  last30Revenue: number;
  avgOrder: number;
  bestProfitDay?: { date: string; profit: number } | null;
  recentRevenue?: number;
};

type InsightInputBundle = {
  sales: SalesDailyRow[];
  telegram: { message_id: string; text: string; published_at: string }[];
  youtubeVideos: {
    title: string;
    url: string;
    publishedAt: string;
    thumbnailUrl?: string | null;
    videoId?: string | null;
  }[];
  youtubeMetrics:
    | {
        viewsToday: number;
        views7d: number;
        views30d: number;
        allTimeViews: number;
        subscribers: number;
        newVideos30d: number;
      }
    | null;
  calendar: { title: string; start: string; end?: string | null; url?: string | null }[];
};

function toDateUtc(dateStr: string): Date | null {
  if (!dateStr) return null;
  const d = new Date(`${dateStr}T00:00:00Z`);
  if (Number.isNaN(d.getTime())) return null;
  return d;
}

function diffDays(from: Date, to: Date): number {
  const ms = to.getTime() - from.getTime();
  return Math.floor(ms / (1000 * 60 * 60 * 24));
}

function computeSalesSummary(rows: SalesDailyRow[]): SalesSummary {
  const now = new Date();
  const last7 = rows.filter((r) => {
    const d = toDateUtc(r.date);
    return d ? diffDays(d, now) <= 6 : false;
  });
  const prev7 = rows.filter((r) => {
    const d = toDateUtc(r.date);
    if (!d) return false;
    const delta = diffDays(d, now);
    return delta >= 7 && delta <= 13;
  });
  const last30 = rows.filter((r) => {
    const d = toDateUtc(r.date);
    return d ? diffDays(d, now) <= 29 : false;
  });

  const sumBlock = (block: SalesDailyRow[]) => {
    return block.reduce(
      (acc, row) => {
        acc.revenue += centsToDollars(row.total_revenue_cents);
        acc.profit += centsToDollars(row.total_profit_cents);
        acc.sales += parseNumber(row.total_sales, 0);
        return acc;
      },
      { revenue: 0, profit: 0, sales: 0 }
    );
  };

  const last7Agg = sumBlock(last7);
  const prev7Agg = sumBlock(prev7);
  const last30Agg = sumBlock(last30);

  let bestProfitDay: { date: string; profit: number } | null = null;
  for (const row of rows) {
    const profit = centsToDollars(row.total_profit_cents);
    if (!bestProfitDay || profit > bestProfitDay.profit) {
      bestProfitDay = { date: row.date, profit };
    }
  }

  const recent = rows[0];
  const avgOrder =
    last7Agg.sales > 0 ? Math.round((last7Agg.revenue / last7Agg.sales) * 100) / 100 : centsToDollars(recent?.avg_profit_cents || 0);

  return {
    last7Revenue: last7Agg.revenue,
    prev7Revenue: prev7Agg.revenue,
    last7Profit: last7Agg.profit,
    last30Revenue: last30Agg.revenue,
    avgOrder,
    bestProfitDay,
    recentRevenue: centsToDollars(recent?.total_revenue_cents ?? 0),
  };
}

async function loadSalesHistory(env: Env): Promise<SalesDailyRow[]> {
  try {
    const { results } = await env.DB.prepare(
      "SELECT date, total_sales, total_revenue_cents, total_profit_cents, avg_profit_cents FROM sales_daily ORDER BY date DESC LIMIT 200"
    ).all<SalesDailyRow>();
    return results ?? [];
  } catch (err) {
    console.error("Failed to load sales history for insights", err);
    return [];
  }
}

async function loadTelegramHistory(env: Env): Promise<{ message_id: string; text: string; published_at: string }[]> {
  try {
    const { results } = await env.DB.prepare(
      "SELECT message_id, text, published_at FROM telegram_posts ORDER BY published_at DESC LIMIT 50"
    ).all<{
      message_id: string;
      text: string;
      published_at: string;
    }>();
    return results ?? [];
  } catch (err) {
    console.error("Failed to load telegram history for insights", err);
    return [];
  }
}

async function loadYoutubeMetrics(env: Env) {
  try {
    const row = await env.DB.prepare(
      "SELECT views_today, views_7d, views_30d, views_all_time, subscribers, new_videos_30d FROM youtube_daily ORDER BY updated_at DESC LIMIT 1"
    ).first<{
      views_today: number;
      views_7d: number;
      views_30d: number;
      views_all_time: number;
      subscribers: number;
      new_videos_30d: number;
    }>();
    if (!row) return null;
    return {
      viewsToday: parseNumber(row.views_today, 0),
      views7d: parseNumber(row.views_7d, 0),
      views30d: parseNumber(row.views_30d, 0),
      allTimeViews: parseNumber(row.views_all_time, 0),
      subscribers: parseNumber(row.subscribers, 0),
      newVideos30d: parseNumber(row.new_videos_30d, 0),
    };
  } catch (err) {
    console.error("Failed to load youtube metrics for insights", err);
    return null;
  }
}

async function collectInsightInputs(env: Env): Promise<InsightInputBundle> {
  const [sales, telegram, youtubeVideos, calendar, youtubeMetrics] = await Promise.all([
    loadSalesHistory(env),
    loadTelegramHistory(env),
    fetchLatestYoutubeVideos(env),
    loadCalendarEvents(env),
    loadYoutubeMetrics(env),
  ]);

  return {
    sales,
    telegram,
    youtubeVideos,
    youtubeMetrics,
    calendar,
  };
}

function trimText(text: string, max = 300) {
  if (!text) return "";
  const cleaned = stripMarkdownAndEmojis(text);
  return cleaned.length > max ? `${cleaned.slice(0, Math.max(0, max - 3))}...` : cleaned;
}

function sanitizeSource(value: any): InsightCard["source"] {
  const allowed = new Set<InsightCard["source"]>(["summary", "ebay", "telegram", "youtube", "calendar"]);
  if (allowed.has(value)) return value;
  const lower = typeof value === "string" ? value.toLowerCase() : "";
  if (lower === "ai_summary" || lower === "summary") return "summary";
  if (allowed.has(lower as any)) return lower as InsightCard["source"];
  return "ebay";
}

function sanitizeType(value: any): InsightCard["type"] {
  const allowed = new Set<InsightCard["type"]>(["money", "margin", "action", "signal", "plan", "recommendation"]);
  if (allowed.has(value)) return value;
  const lower = typeof value === "string" ? value.toLowerCase() : "";
  if (allowed.has(lower as any)) return lower as InsightCard["type"];
  return "action";
}

function sanitizePeriod(value: any): InsightCard["period"] {
  const allowed = new Set<InsightCard["period"]>(["today", "7d", "week", "30d", "3d"]);
  if (allowed.has(value)) return value;
  const lower = typeof value === "string" ? value.toLowerCase() : "";
  if (allowed.has(lower as any)) return lower as InsightCard["period"];
  return "7d";
}

function buildFallbackInsights(runDate: string, summary: SalesSummary, inputs: InsightInputBundle): InsightCard[] {
  const nowIso = new Date().toISOString();
  const delta = summary.last7Revenue - summary.prev7Revenue;
  const deltaPct = summary.prev7Revenue > 0 ? Math.round((delta / summary.prev7Revenue) * 100) : 0;

  const telegramSample = inputs.telegram[0];
  const youtubeSample = inputs.youtubeVideos[0];
  const calendarSample = inputs.calendar[0];
  const telegramTopic = telegramSample ? trimText(telegramSample.text, 120) : "";
  const telegramHasLatin = /[A-Za-z]{6,}/.test(telegramTopic);
  const telegramTextPart = telegramHasLatin ? "" : telegramTopic ? `: "${telegramTopic}"` : "";

  const base: InsightCard[] = [
    {
      id: crypto.randomUUID(),
      createdAt: nowIso,
      runDate,
      source: "ebay",
      type: "money",
      period: "7d",
      title: "Выручка 7д vs пред. 7д",
      text: trimText(
        `Выручка ${summary.last7Revenue.toFixed(0)} против ${summary.prev7Revenue.toFixed(0)} за прошлые 7д (${delta >= 0 ? "+" : ""}${deltaPct}%). Держи средний чек около $${summary.avgOrder.toFixed(2)}.`
      ),
      actions: [],
    },
    {
      id: crypto.randomUUID(),
      createdAt: nowIso,
      runDate,
      source: "ebay",
      type: "action",
      period: "7d",
      title: "Подними маржу в лучший день",
      text: trimText(
        summary.bestProfitDay
          ? `Лучший день по прибыли ${summary.bestProfitDay.date}: $${summary.bestProfitDay.profit.toFixed(
              0
            )}. Повтори промо и подними цены на топ-товары на +3–5%.`
          : "Повтори лучший день прошлой недели и подними цены на топ-товары на +3–5%."
      ),
      actions: [
        "Повтори промо на товарах с лучшей прибылью",
        "Сделай тест +3–5% к цене",
        "Обнови фото и ключи в топ-5 товаров",
      ],
    },
    {
      id: crypto.randomUUID(),
      createdAt: nowIso,
      runDate,
      source: "ebay",
      type: "action",
      period: "7d",
      title: "Перелистни медленные товары",
      text: trimText("Освежи ассортимент: обнови фото и описания на медленных позициях, добавь небольшую скидку на доставку на 48 часов."),
      actions: ["Обнови заголовки и фото на медленных позициях", "Введи 48ч скидку на доставку", "Проверь, остались ли неактуальные товары"],
    },
    {
      id: crypto.randomUUID(),
      createdAt: nowIso,
      runDate,
      source: "ebay",
      type: "action",
      period: "7d",
      title: "Сократи расходы на доставку",
      text: trimText(
        "Проверь товары с низкой маржой и пересчитай доставку/цену, чтобы не уходить в минус на следующей неделе."
      ),
      actions: ["Проверь тарифы доставки и упаковку", "Добавь +3–5% к цене на дорогих отправках", "Тестируй бандлы вместо одиночных отправок"],
    },
    {
      id: crypto.randomUUID(),
      createdAt: nowIso,
      runDate,
      source: "ebay",
      type: "money",
      period: "7d",
      title: "Средний чек под контролем",
      text: trimText(
        `Средний чек сейчас ~$${summary.avgOrder.toFixed(2)}. Поддержи его: акции на доптовары и небольшое повышение цен на популярные позиции.`
      ),
      actions: ["Предложи комплект/доптовар к основному", "Тестируй +2–3% к цене на популярных позициях"],
    },
  ];

  if (telegramSample) {
    base.push({
      id: crypto.randomUUID(),
      createdAt: nowIso,
      runDate,
      source: "telegram",
      type: "signal",
      period: "today",
      title: "Повтори тему из Telegram",
      text: trimText(`Тема дня в Telegram${telegramTextPart}. Перенеси формулировку в заголовок и описание лота.`),
      actions: ["Вставь фразу в заголовок лота", "Добавь ссылку на пост в описании", "Поставь изображение под тему"],
    });
  }

  if (youtubeSample) {
    base.push({
      id: crypto.randomUUID(),
      createdAt: nowIso,
      runDate,
      source: "youtube",
      type: "signal",
      period: "7d",
      title: "Рост интереса на YouTube",
      text: trimText(`Последнее видео: "${trimText(youtubeSample.title, 140)}". Добавь связанный аксессуар как апселл.`),
      actions: ["Прикрепи товар под видео", "Добавь купон в описание", "Повесь баннер магазина под тему видео"],
    });
  }

  if (calendarSample) {
    base.push({
      id: crypto.randomUUID(),
      createdAt: nowIso,
      runDate,
      source: "calendar",
      type: "plan",
      period: "week",
      title: "Запланируй eBay-блок",
      text: trimText(
        `Заблокируй 90 минут перед событием "${trimText(calendarSample.title || "событие", 40)}", чтобы отправить заказы и обновить цены.`
      ),
      actions: ["Добавь слот 90 минут в календарь", "Сделай рассылку/обновление цен пачкой", "Проверь напоминания на телефоне"],
    });
  }

  return base;
}

function extractKeywords(text: string): string[] {
  const stop = new Set([
    "https",
    "http",
    "video",
    "watch",
    "about",
    "httpswww",
    "today",
    "сегодня",
    "httpwww",
    "aipost",
    "tme",
    "you",
    "your",
    "free",
  ]);
  const cleaned = stripMarkdownAndEmojis(text).toLowerCase();
  return cleaned
    .split(/[^a-zа-я0-9ё]+/i)
    .map((t) => t.trim())
    .filter((t) => t.length >= 5 && !stop.has(t));
}

function buildTelegramCards(posts: { text: string; published_at: string }[], runDate: string): InsightCard[] {
  if (!posts || posts.length === 0) return [];
  const nowIso = new Date().toISOString();

  const toShortTopic = (text: string): string => {
    const stop = new Set([
      "and",
      "the",
      "with",
      "without",
      "into",
      "about",
      "this",
      "that",
      "there",
      "here",
      "today",
      "http",
      "https",
      "www",
      "какой",
      "какая",
      "какие",
      "что",
      "это",
      "как",
      "для",
      "или",
      "чтобы",
      "from",
      "your",
      "you",
      "any",
      "apps",
      "officially",
      "wikipedia",
      "reporter",
      "problem",
    ]);
    const words = stripMarkdownAndEmojis(text)
      .toLowerCase()
      .split(/\s+/)
      .map((w) => w.replace(/[^a-zа-я0-9ё-]/gi, ""))
      .filter((w) => w.length >= 3 && !stop.has(w))
      .slice(0, 3);
    const phrase = words.join(" ");
    const capitalized = phrase ? phrase.charAt(0).toUpperCase() + phrase.slice(1) : "Тема";
    return trimText(capitalized || "Тема", 40);
  };

  const top3: string[] = [];
  for (const p of posts.slice(0, 3)) {
    const t = toShortTopic(p.text || "");
    if (t) top3.push(t);
  }
  while (top3.length < 3) top3.push("тема дня");

  // повтор темы 3 дня подряд (если доступно)
  const byDateTokens = new Map<string, Set<string>>();
  for (const p of posts.slice(0, 40)) {
    const date = (p.published_at || "").slice(0, 10);
    if (!date) continue;
    const tokens = new Set(extractKeywords(p.text || ""));
    if (!tokens.size) continue;
    if (!byDateTokens.has(date)) byDateTokens.set(date, new Set<string>());
    const set = byDateTokens.get(date)!;
    for (const t of tokens) set.add(t);
  }

  const tokenDates = new Map<string, Set<string>>();
  for (const [date, tokens] of byDateTokens.entries()) {
    for (const t of tokens) {
      if (!tokenDates.has(t)) tokenDates.set(t, new Set<string>());
      tokenDates.get(t)!.add(date);
    }
  }

  let repeatedTopic: string | null = null;
  for (const [token, dates] of tokenDates.entries()) {
    if (dates.size >= 3) {
      repeatedTopic = token;
      break;
    }
  }

  const topicsText = `Темы:\n1) ${top3[0] || "—"}\n2) ${top3[1] || "—"}\n3) ${top3[2] || "—"}`;
  const repeatedText = repeatedTopic ? `\nПовторяется 3 дня: ${repeatedTopic}.` : "";
  const conclusion = "Сигнал: выбери одну тему и подумай, почему она важна.";

  const cards: InsightCard[] = [
    {
      id: crypto.randomUUID(),
      createdAt: nowIso,
      runDate,
      source: "telegram",
      type: "recommendation",
      period: "today",
      title: "Темы дня в Telegram",
      text: `${topicsText}${repeatedText}\n${conclusion}`,
      actions: [
        "Выбери 1 тему и сформулируй 1 вопрос/гипотезу",
        "Сформируй 3 ключевые фразы/угла подачи по теме",
      ],
    },
  ];

  return cards.slice(0, INSIGHTS_MAX_TELEGRAM);
}

function buildYoutubeCard(videos: { title: string; url: string; publishedAt: string }[], runDate: string): InsightCard[] {
  if (!videos || videos.length === 0) return [];
  const nowIso = new Date().toISOString();
  const list = videos.slice(0, 3).map((v, idx) => `${idx + 1}) ${trimText(v.title, 80)}`);
  const priority = `Приоритет: сначала №2, потом №1, наименее важное №3.`;
  const text = `Что нового:\n${list.join("\n")}\n${priority}`;
  return [
    {
      id: crypto.randomUUID(),
      createdAt: nowIso,
      runDate,
      source: "youtube",
      type: "recommendation",
      period: "week",
      title: "Что нового на YouTube",
      text,
      actions: [
        "Выбери №1 или №2 и выпиши 1 идею/приём",
        "Сформулируй 1 фразу/термин, который стоит запомнить",
      ],
    },
  ];
}

function buildCalendarCard(events: { title: string; start: string; end: string | null }[], runDate: string): InsightCard[] {
  if (!events) events = [];
  const nowIso = new Date().toISOString();
  const items = events.slice(0, 3).map((ev) => {
    const start = ev.start ? new Date(ev.start) : null;
    const day = start
      ? start.toLocaleDateString("ru-RU", { weekday: "short", day: "2-digit", month: "2-digit" })
      : "дата";
    const time = start ? start.toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit" }) : "—";
    return `• ${day} ${time} — ${trimText(ev.title || "Событие", 40)}`;
  });

  const text =
    items.length > 0
      ? `Ближайшие события:\n${items.join("\n")}`
      : "На ближайшие 3 дня событий нет — можно спокойно планировать.";
  const actions: string[] = ["Поставить напоминание за 30 минут", "Добавить заметку к занятию (что подготовить)"];

  return [
    {
      id: crypto.randomUUID(),
      createdAt: nowIso,
      runDate,
      source: "calendar",
      type: "plan",
      period: "3d",
      title: "Расписание на 3 дня",
      text,
      actions,
    },
  ];
}

function normalizeInsights(
  raw: any[],
  runDate: string,
  context: { summary: SalesSummary; inputs: InsightInputBundle }
): InsightCard[] {
  const nowIso = new Date().toISOString();
  const fromAi: InsightCard[] = [];

  for (const item of Array.isArray(raw) ? raw : []) {
    const card: InsightCard = {
      id: crypto.randomUUID(),
      createdAt: nowIso,
      runDate,
      source: sanitizeSource(item.source),
      type: sanitizeType(item.type),
      period: sanitizePeriod(item.period),
      title: sanitizeTitleField(item.title || "Инсайт"),
      text: sanitizeTextField(item.text || "", MAX_TEXT_LENGTH),
      actions: sanitizeActionsField(item.actions),
    };
    fromAi.push(card);
  }

  const fallbackPool = buildFallbackInsights(runDate, context.summary, context.inputs);
  const ebayFallback = fallbackPool.filter((c) => c.source === "ebay");
  const telegramFallback = buildTelegramCards(context.inputs.telegram, runDate);
  const youtubeFallback = buildYoutubeCard(context.inputs.youtubeVideos, runDate);
  const calendarFallback = buildCalendarCard(context.inputs.calendar, runDate);

  const result: InsightCard[] = [];
  const usedTitles = new Set<string>();
  const pushUnique = (card: InsightCard) => {
    if (!card.title) return;
    if (usedTitles.has(card.title)) return;
    usedTitles.add(card.title);
    result.push(card);
  };

  const addFromPool = (pool: InsightCard[], limit: number, predicate?: (c: InsightCard) => boolean) => {
    for (const c of pool) {
      if (result.length >= INSIGHTS_MAX || limit <= 0) break;
      if (predicate && !predicate(c)) continue;
      pushUnique({ ...c, id: crypto.randomUUID(), createdAt: nowIso, runDate });
      limit -= 1;
    }
  };

  // eBay first (money then action)
  const ebayAi = fromAi.filter((c) => c.source === "ebay");
  const ebayMoneyPool = [...ebayAi, ...ebayFallback].filter((c) => c.type === "money");
  const ebayActionPool = [...ebayAi, ...ebayFallback].filter((c) => c.type === "action");
  addFromPool(ebayMoneyPool, INSIGHTS_MONEY_TARGET);
  addFromPool(ebayActionPool, INSIGHTS_MAX_EBAY - result.filter((c) => c.source === "ebay").length);
  // if still short, fill with any ebay to reach 5
  const ebayShort = result.filter((c) => c.source === "ebay").length;
  if (ebayShort < INSIGHTS_MAX_EBAY) {
    addFromPool([...ebayMoneyPool, ...ebayActionPool], INSIGHTS_MAX_EBAY - ebayShort);
  }
  // ensure action count
  const actionsNow = result.filter((c) => c.source === "ebay" && c.type === "action").length;
  if (actionsNow < INSIGHTS_MIN_ACTION) {
    addFromPool(ebayActionPool, INSIGHTS_MIN_ACTION - actionsNow, (c) => c.type === "action");
  }

  // Telegram 1-2
  const telegramAi = fromAi.filter((c) => c.source === "telegram");
  const telePool = [...telegramAi, ...telegramFallback];
  const teleTarget = telePool.length > 0 ? 1 : 0;
  addFromPool(telePool, teleTarget);

  // YouTube 1
  const youtubeAi = fromAi.filter((c) => c.source === "youtube");
  if (youtubeAi.length > 0) addFromPool(youtubeAi, 1);
  else addFromPool(youtubeFallback, 1);

  // Calendar 0-1
  const calendarAi = fromAi.filter((c) => c.source === "calendar");
  if (calendarAi.length > 0) addFromPool(calendarAi, 1);
  else addFromPool(calendarFallback, 1);

  return result.slice(0, INSIGHTS_MAX);
}

function buildInsightsPrompt(inputs: InsightInputBundle, summary: SalesSummary): string {
  const promptPayload = {
    sales_summary: summary,
    sales_points: inputs.sales.slice(0, 40),
    telegram_posts: inputs.telegram.slice(0, 15).map((p) => ({
      text: trimText(p.text, 180),
      published_at: p.published_at,
    })),
    youtube_latest: inputs.youtubeVideos.slice(0, 5).map((v) => ({
      title: trimText(v.title, 140),
      publishedAt: v.publishedAt,
    })),
    calendar_upcoming: inputs.calendar.slice(0, 8),
  };

  return `
You are an e-commerce copilot for Irina. Generate concise insight cards for her dashboard.
Return STRICT JSON ONLY: an array with 6-8 objects, nothing else.

Schema:
[
  {
    "source": "ebay" | "telegram" | "youtube" | "calendar",
    "type": "money" | "margin" | "action" | "signal" | "plan",
    "period": "today" | "7d" | "week" | "30d",
    "title": "до 60 символов",
    "text": "до 300 символов",
    "actions": ["до 3 шагов"]  // всегда массив, даже если пустой
  }
]

Rules:
- 6-8 карточек.
- Минимум 4 карточки source="ebay". Минимум 2 карточки type="action".
- Язык: русский. Без HTML-сущностей, без markdown, без эмодзи.
- Не придумывай item_id/SKU/title, если нет в данных. Используй только агрегаты (дни, недели, суммы).
- Формулируй как инсайт + конкретное действие (коротко).
- Telegram: 1 карточка, 3 короткие темы дня + один вывод (без привязки к eBay).
- YouTube: 1 карточка по последним видео: что нового + что посмотреть Ирине (одна рекомендация).
- Calendar: 0-1 карточка, только если есть события (свободные слоты/перегруз), без привязки к eBay.
- Если данных мало по источнику — не выдумывай, лучше пропусти.

Data (JSON):
${JSON.stringify(promptPayload, null, 2)}
`;
}

async function generateInsightsWithGemini(env: Env, inputs: InsightInputBundle, summary: SalesSummary): Promise<any[]> {
  if (!env.GEMINI_API_KEY) return [];
  const prompt = buildInsightsPrompt(inputs, summary);
  const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${env.GEMINI_API_KEY}`;

  try {
    const res = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        contents: [{ role: "user", parts: [{ text: prompt }]}],
        generationConfig: {
          temperature: 0.5,
          maxOutputTokens: 800,
          responseMimeType: "application/json",
        },
      }),
    });

    const text = await res.text();
    if (!res.ok) {
      console.error("Gemini returned error", res.status, text.slice(0, 500));
      return [];
    }

    let payload: any = null;
    try {
      payload = JSON.parse(text);
    } catch (_) {
      payload = text;
    }

    const rawText =
      payload?.candidates?.[0]?.content?.parts?.[0]?.text ??
      payload?.candidates?.[0]?.content?.parts?.[0]?.text ??
      (typeof payload === "string" ? payload : "");

    if (!rawText) return [];

    try {
      const parsed = JSON.parse(rawText);
      return Array.isArray(parsed) ? parsed : [];
    } catch (err) {
      console.error("Failed to parse Gemini JSON", err);
      return [];
    }
  } catch (err) {
    console.error("Gemini fetch failed", err);
    return [];
  }
}

type SummaryFacts = {
  hasSalesData: boolean;
  metricPhrases: string[];
  missing: string[];
};

function buildSummaryFacts(summary: SalesSummary, salesRows: number): SummaryFacts {
  const hasSalesData = salesRows > 0;
  const metricPhrases: string[] = [];
  const missing: string[] = [];

  if (!hasSalesData) {
    missing.push("нет данных по продажам");
  } else {
    metricPhrases.push(`выручка 7д: $${summary.last7Revenue.toFixed(0)}`);
    metricPhrases.push(`выручка пред. 7д: $${summary.prev7Revenue.toFixed(0)}`);
    metricPhrases.push(`прибыль 7д: $${summary.last7Profit.toFixed(0)}`);
    metricPhrases.push(`средний чек: $${summary.avgOrder.toFixed(2)}`);
    metricPhrases.push(`выручка 30д: $${summary.last30Revenue.toFixed(0)}`);
  }

  return { hasSalesData, metricPhrases, missing };
}

function buildSummaryPrompt(cards: InsightCard[], facts: SummaryFacts): string {
  const payload = cards.map((card) => ({
    source: card.source,
    type: card.type,
    period: card.period,
    title: card.title,
    text: card.text,
    actions: card.actions ?? [],
  }));

  return `
Ты делаешь итоговую сводку по уже сгенерированным карточкам.
Верни STRICT JSON ONLY — один объект, без массива и без дополнительного текста.

Schema:
{
  "source": "summary",
  "type": "recommendation",
  "period": "today",
  "title": "Сводка",
  "text": "многострочный текст",
  "actions": ["ровно 2 коротких действия"]
}

Rules:
- Язык: русский. Без HTML/markdown/эмодзи.
- title <= 60, text <= 300.
- text (строго по форме):
  Итог: 1–2 предложения
  Топ-3 действия:
  1) ...
  2) ...
  3) ...
  Риски/что проверить: 1–2 пункта
- В строке "Итог" используй минимум 2 фразы из metric_phrases ДОСЛОВНО.
- Если has_sales_data=false, напиши: "Данных недостаточно: ..." используя missing.
- Формулируй на "ты", без 1-го лица.
- В "Топ-3 действия" используй глаголы: Сделай/Проверь/Сравни/Обнови.
- Добавь 1–2 элемента конкретики (период/число), но без воды.
- actions: ровно 2, короткие, практичные, тоже в стиле "ты".
- НЕ добавляй строки "Учёба" и "Домашка" — они будут добавлены отдельно.

Facts (JSON):
${JSON.stringify(
  {
    has_sales_data: facts.hasSalesData,
    metric_phrases: facts.metricPhrases,
    missing: facts.missing,
  },
  null,
  2
)}

Cards (JSON):
${JSON.stringify(payload, null, 2)}
`;
}

async function generateSummaryWithGemini(
  env: Env,
  cards: InsightCard[],
  studyBlock: string,
  facts: SummaryFacts
): Promise<InsightCard | null> {
  if (!env.GEMINI_API_KEY || cards.length === 0) return null;
  const prompt = buildSummaryPrompt(cards, facts);
  const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${env.GEMINI_API_KEY}`;

  try {
    const res = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        contents: [{ role: "user", parts: [{ text: prompt }]}],
        generationConfig: {
          temperature: 0.3,
          maxOutputTokens: 400,
          responseMimeType: "application/json",
        },
      }),
    });

    const text = await res.text();
    if (!res.ok) {
      console.error("Gemini summary error", res.status, text.slice(0, 500));
      return null;
    }

    let payload: any = null;
    try {
      payload = JSON.parse(text);
    } catch (_) {
      payload = text;
    }

    const rawText =
      payload?.candidates?.[0]?.content?.parts?.[0]?.text ??
      payload?.candidates?.[0]?.content?.parts?.[0]?.text ??
      (typeof payload === "string" ? payload : "");

    if (!rawText) return null;

    let parsed: any = null;
    if (typeof rawText === "string") {
      try {
        parsed = JSON.parse(rawText);
      } catch (err) {
        console.error("Failed to parse Gemini summary JSON", err);
        return null;
      }
    } else if (rawText && typeof rawText === "object") {
      parsed = rawText;
    } else {
      return null;
    }

    let actions = sanitizeActionsField(parsed?.actions);
    if (actions.length < 2) {
      actions = [
        "Выдели 1 главный фокус дня и отложи второстепенное",
        "Сформулируй критерий успеха на сегодня (1 строка)",
      ];
    }

    const cleanedText = sanitizeMultilineText(parsed?.text ?? "", MAX_SUMMARY_TEXT_LENGTH);
    const hasStructure =
      cleanedText.includes("Итог:") &&
      cleanedText.includes("Топ-3 действия") &&
      cleanedText.includes("Риски/что проверить");
    const usesMetrics = facts.hasSalesData
      ? facts.metricPhrases.filter((phrase) => cleanedText.includes(phrase)).length >= 2
      : /данных недостаточно/i.test(cleanedText);
    const fallback = buildFallbackSummary(cards, getRunDate(), studyBlock, facts);
    const composedText = hasStructure
      ? sanitizeMultilineText([studyBlock, cleanedText].filter(Boolean).join("\n"), MAX_SUMMARY_TEXT_LENGTH)
      : fallback.text;
    return {
      id: crypto.randomUUID(),
      createdAt: new Date().toISOString(),
      runDate: getRunDate(),
      source: "summary",
      type: "recommendation",
      period: "today",
      title: "Сводка",
      text: hasStructure && usesMetrics ? composedText : fallback.text,
      actions: actions.slice(0, 2),
      inputDigest: null,
    };
  } catch (err) {
    console.error("Gemini summary fetch failed", err);
    return null;
  }
}

function buildFallbackSummary(
  cards: InsightCard[],
  runDate: string,
  studyBlock: string,
  facts: SummaryFacts
): InsightCard {
  const nowIso = new Date().toISOString();
  const titles = cards.map((c) => c.title).filter(Boolean).slice(0, 3);
  while (titles.length < 3) titles.push("Фокус дня");

  const metricsLine =
    facts.metricPhrases.length >= 2 ? `(${facts.metricPhrases.slice(0, 2).join(", ")})` : "";
  const missingLine =
    !facts.hasSalesData && facts.missing.length > 0 ? `Данных недостаточно: ${facts.missing.join(", ")}.` : "";

  const body = [
    missingLine
      ? `Итог: ${missingLine}`
      : `Итог: фокус дня — eBay и свежесть внешних сигналов; календарь держи как план. ${metricsLine}`.trim(),
    "Топ-3 действия:",
    "1) Сравни 7д vs пред. 7д и зафиксируй причину.",
    "2) Проверь средний чек и главный источник кассы за 7д.",
    "3) Обнови сбор, если обновление старше 24 ч.",
    "Риски/что проверить: несвежие данные, сбои источников.",
  ].join("\n");
  const text = sanitizeMultilineText(
    [studyBlock, body].filter(Boolean).join("\n"),
    MAX_SUMMARY_TEXT_LENGTH
  );

  return {
    id: crypto.randomUUID(),
    createdAt: nowIso,
    runDate,
    source: "summary",
    type: "recommendation",
    period: "today",
    title: "Сводка",
    text,
    actions: [
      "Выдели 1 главный фокус дня и отложи второстепенное",
      "Сформулируй критерий успеха на сегодня (1 строка)",
    ],
    inputDigest: null,
  };
}

async function generateAndStoreInsights(env: Env, trigger: string) {
  const runDate = getRunDate();
  const inputs = await collectInsightInputs(env);
  const summary = computeSalesSummary(inputs.sales);
  const aiRaw = await generateInsightsWithGemini(env, inputs, summary);

  const insights = normalizeInsights(aiRaw, runDate, { summary, inputs });
  const studyBlock = buildStudyBlock(inputs.calendar);
  const summaryFacts = buildSummaryFacts(summary, inputs.sales.length);
  const summaryCard = await generateSummaryWithGemini(env, insights, studyBlock, summaryFacts);
  const merged = [summaryCard ?? buildFallbackSummary(insights, runDate, studyBlock, summaryFacts), ...insights];
  const digest = await buildInputDigest({
    runDate,
    trigger,
    summary,
    sales: inputs.sales.slice(0, 60),
    telegram: inputs.telegram.slice(0, 20),
    youtube: inputs.youtubeVideos.slice(0, 5),
    calendar: inputs.calendar.slice(0, 8),
  });

  await storeInsights(env, runDate, merged, digest);

  return {
    runDate,
    generatedAt: new Date().toISOString(),
    insights: merged,
  };
}

async function handleInsightsLatest(env: Env): Promise<Response> {
  try {
    const latest = await loadLatestInsights(env);
    if (latest.insights.length > 0) return jsonResponse(latest, 200, "no-store");

    const generated = await generateAndStoreInsights(env, "latest-fallback");
    return jsonResponse({ runDate: generated.runDate, insights: generated.insights }, 200, "no-store");
  } catch (e: any) {
    console.error("handleInsightsLatest error:", e.stack || e.message || e);
    try {
      const runDate = getRunDate();
      const inputs =
        (await collectInsightInputs(env).catch(() => null)) ?? {
          sales: [],
          telegram: [],
          youtubeVideos: [],
          youtubeMetrics: null,
          calendar: [],
        };
      const summary = computeSalesSummary(inputs.sales ?? []);
      const fallback = normalizeInsights([], runDate, { summary, inputs });
      return jsonResponse({
        runDate,
        insights: fallback,
        error: "fallback",
        message: String(e?.message || e),
      }, 200, "no-store");
    } catch (fallbackErr: any) {
      console.error("handleInsightsLatest fallback failed:", fallbackErr.stack || fallbackErr.message || fallbackErr);
      return jsonResponse({ error: "Internal error", message: String(e?.message || e) }, 500, "no-store");
    }
  }
}

async function handleInsightsGenerate(env: Env): Promise<Response> {
  try {
    const generated = await generateAndStoreInsights(env, "manual");
    return jsonResponse({ runDate: generated.runDate, insights: generated.insights }, 200, "no-store");
  } catch (e: any) {
    console.error("handleInsightsGenerate error:", e.stack || e.message || e);
    try {
      const runDate = getRunDate();
      const inputs =
        (await collectInsightInputs(env).catch(() => null)) ?? {
          sales: [],
          telegram: [],
          youtubeVideos: [],
          youtubeMetrics: null,
          calendar: [],
        };
      const summary = computeSalesSummary(inputs.sales ?? []);
      const fallback = normalizeInsights([], runDate, { summary, inputs });
      return jsonResponse({
        runDate,
        insights: fallback,
        error: "fallback",
        message: String(e?.message || e),
      });
    } catch (fallbackErr: any) {
      console.error("handleInsightsGenerate fallback failed:", fallbackErr.stack || fallbackErr.message || fallbackErr);
      return jsonResponse({ error: "Internal error", message: String(e.message || e), stack: e.stack }, 500);
    }
  }
}

function withTimeout<T>(promise: Promise<T>, ms: number, label = "task"): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
    promise
      .then((v) => {
        clearTimeout(timer);
        resolve(v);
      })
      .catch((e) => {
        clearTimeout(timer);
        reject(e);
      });
  });
}

function decodeHtmlEntities(text: string) {
  return text
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'");
}

function daysAgoIso(days: number) {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() - days);
  return d.toISOString();
}

async function ensureCalendarTable(env: Env) {
  await env.DB.prepare(
    `CREATE TABLE IF NOT EXISTS calendar_events (
       id INTEGER PRIMARY KEY AUTOINCREMENT,
       event_id TEXT UNIQUE,
       summary TEXT,
       start_time TEXT,
       end_time TEXT,
       html_link TEXT,
       calendar_id TEXT,
       created_at TEXT DEFAULT (datetime('now'))
     )`
  ).run();
}

function toIso(value: string | undefined | null): string | null {
  if (!value) return null;
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString();
}

async function refreshCalendar(env: Env) {
  if (!env.COMPOSIO_API_KEY || !env.COMPOSIO_CALENDAR_ACCOUNT_ID || !env.COMPOSIO_CALENDAR_ENTITY_ID) {
    return;
  }

  const now = new Date();
  const timeMin = now.toISOString();
  const maxDate = new Date(now);
  maxDate.setUTCDate(maxDate.getUTCDate() + 30);
  const timeMax = maxDate.toISOString();

  try {
    await ensureCalendarTable(env);
    const res = await fetch("https://backend.composio.dev/api/v3/tools/execute/GOOGLESUPER_FIND_EVENT", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-api-key": env.COMPOSIO_API_KEY,
      },
      body: JSON.stringify({
        connected_account_id: env.COMPOSIO_CALENDAR_ACCOUNT_ID,
        entity_id: env.COMPOSIO_CALENDAR_ENTITY_ID,
        arguments: {
          calendar_id: "primary",
          timeMin,
          timeMax,
          single_events: true,
          order_by: "startTime",
          max_results: 50,
        },
      }),
    });

    const text = await res.text();
    if (!res.ok) {
      console.error("Calendar fetch failed:", res.status, text.slice(0, 800));
      return;
    }
    let payload: any = null;
    try {
      payload = JSON.parse(text);
    } catch (e) {
      console.error("Calendar response is not JSON:", text.slice(0, 800));
      return;
    }
    const items =
      payload?.data?.event_data?.event_data ||
      payload?.data?.items ||
      payload?.data?.events ||
      payload?.items ||
      [];

    if (!Array.isArray(items) || items.length === 0) return;

    await env.DB.prepare("DELETE FROM calendar_events").run();
    const insert = env.DB.prepare(
      "INSERT OR REPLACE INTO calendar_events (event_id, summary, start_time, end_time, html_link, calendar_id) VALUES (?, ?, ?, ?, ?, ?)"
    );

    for (const ev of items.slice(0, 50)) {
      const eventId = ev.id || ev.event_id;
      if (!eventId) continue;
      const summary = ev.summary || ev.title || "Event";
      const startRaw = ev.start?.dateTime || ev.start?.date || ev.start_time;
      const endRaw = ev.end?.dateTime || ev.end?.date || ev.end_time;
      const startIso = toIso(startRaw);
      const endIso = toIso(endRaw);
      if (!startIso) continue;
      const link = ev.htmlLink || ev.html_link || null;
      const calId = ev.organizer?.email || ev.calendar_id || "primary";
      await insert.bind(eventId, summary, startIso, endIso, link, calId).run();
    }
  } catch (err) {
    console.error("Failed to refresh calendar", err);
  }
}

async function loadCalendarEvents(env: Env) {
  try {
    await ensureCalendarTable(env);
    const nowIso = new Date().toISOString();
    const { results } = await env.DB.prepare(
      "SELECT summary, start_time, end_time, html_link FROM calendar_events WHERE start_time >= ? ORDER BY start_time ASC LIMIT 20"
    )
      .bind(nowIso)
      .all<{
        summary: string;
        start_time: string;
        end_time: string | null;
        html_link: string | null;
      }>();
    return (results || []).map((r) => ({
      title: r.summary,
      start: r.start_time,
      end: r.end_time,
      url: r.html_link ?? null,
    }));
  } catch (err) {
    console.error("Failed to load calendar events", err);
    return [];
  }
}

async function resolveChannelIdFromHandle(env: Env, handle: string): Promise<string | null> {
  const sanitized = handle.replace(/^@/, "");
  try {
    const res = await fetch(`https://www.youtube.com/@${sanitized}`, {
      headers: { "user-agent": "Mozilla/5.0 (compatible; D1Worker/1.0)" },
    });
    if (!res.ok) return null;
    const html = await res.text();
    const matchId =
      html.match(/"channelId":"(UC[^"]+)"/)?.[1] ||
      html.match(/href="\/channel\/([^"]+)"/)?.[1] ||
      html.match(/data-channel-external-id="(UC[^"]+)"/)?.[1];
    return matchId || null;
  } catch (e) {
    console.error("Failed to resolve channel id from handle", e);
    return null;
  }
}

async function fetchLatestYoutubeVideos(env: Env): Promise<
  {
    title: string;
    url: string;
    publishedAt: string;
    thumbnailUrl?: string | null;
    videoId?: string | null;
  }[]
> {
  const handle = env.YOUTUBE_SOURCE_HANDLE || "Varlamov.Travel";
  const channelId =
    env.YOUTUBE_SOURCE_CHANNEL_ID ||
    env.COMPOSIO_YT_CHANNEL_ID ||
    (await resolveChannelIdFromHandle(env, handle));

  if (!channelId) return [];

  const ensureCache = async () => {
    try {
      await env.DB.prepare(
        `CREATE TABLE IF NOT EXISTS youtube_videos_cache (
           id INTEGER PRIMARY KEY AUTOINCREMENT,
           fetched_at TEXT,
           channel_id TEXT,
           videos_json TEXT
         )`
      ).run();
    } catch (_) {}
  };

  const saveCache = async (videos: any[]) => {
    try {
      await ensureCache();
      await env.DB.prepare("DELETE FROM youtube_videos_cache WHERE channel_id = ?").bind(channelId).run();
      await env.DB.prepare(
        "INSERT INTO youtube_videos_cache (fetched_at, channel_id, videos_json) VALUES (?, ?, ?)"
      )
        .bind(new Date().toISOString(), channelId, JSON.stringify(videos))
        .run();
    } catch (err) {
      console.error("Failed to cache youtube videos", err);
    }
  };

  const loadCache = async () => {
    try {
      await ensureCache();
      const row = await env.DB.prepare(
        "SELECT fetched_at, videos_json FROM youtube_videos_cache WHERE channel_id = ? ORDER BY fetched_at DESC LIMIT 1"
      )
        .bind(channelId)
        .first<{ fetched_at: string; videos_json: string }>();
      if (!row?.videos_json) return [];
      const ageDays =
        (Date.now() - new Date(row.fetched_at).getTime()) / (1000 * 60 * 60 * 24);
      if (ageDays > YT_CACHE_TTL_DAYS) return [];
      const parsed = JSON.parse(row.videos_json);
      return Array.isArray(parsed) ? parsed.slice(0, 3) : [];
    } catch (err) {
      console.error("Failed to load youtube cache", err);
      return [];
    }
  };

  const demoVideos = [
    {
      title: "5 Must-Know Greek Words: Quality and Distance",
      url: "https://www.youtube.com/watch?v=J8uc4KVxI0c",
      publishedAt: "2026-01-18T09:30:09+00:00",
      thumbnailUrl: "https://i3.ytimg.com/vi/J8uc4KVxI0c/hqdefault.jpg",
      videoId: "J8uc4KVxI0c",
    },
    {
      title: "How is Your Greek? Talking about Your Greek Language Skills",
      url: "https://www.youtube.com/watch?v=ok9vq-ncTKI",
      publishedAt: "2026-01-16T09:30:28+00:00",
      thumbnailUrl: "https://i4.ytimg.com/vi/ok9vq-ncTKI/hqdefault.jpg",
      videoId: "ok9vq-ncTKI",
    },
    {
      title: "Want to Learn Greek Anywhere, Anytime on Your Mobile and For FREE?",
      url: "https://www.youtube.com/watch?v=T216YYY36rE",
      publishedAt: "2026-01-15T09:30:28+00:00",
      thumbnailUrl: "https://i1.ytimg.com/vi/T216YYY36rE/hqdefault.jpg",
      videoId: "T216YYY36rE",
    },
  ];

  const fetchFromHtml = async () => {
    try {
      const res = await fetch(`https://www.youtube.com/channel/${channelId}/videos`, {
        headers: { "user-agent": "Mozilla/5.0 (compatible; D1Worker/1.0)" },
      });
      if (!res.ok) throw new Error(`HTML status ${res.status}`);
      const html = await res.text();
      const entries: {
        title: string;
        url: string;
        publishedAt: string;
        thumbnailUrl?: string | null;
        videoId?: string | null;
      }[] = [];
      const regex =
        /"videoId":"([^"]{11})"[\s\S]{0,200}?"title":\{"runs":\[\{"text":"([^"]+)"/g;
      let match: RegExpExecArray | null;
      const seen = new Set<string>();
      while ((match = regex.exec(html)) && entries.length < 3) {
        const videoId = match[1];
        if (seen.has(videoId)) continue;
        seen.add(videoId);
        const title = decodeHtmlEntities(match[2] || "Video");
        entries.push({
          title,
          url: `https://www.youtube.com/watch?v=${videoId}`,
          publishedAt: "",
          thumbnailUrl: `https://i3.ytimg.com/vi/${videoId}/hqdefault.jpg`,
          videoId,
        });
      }
      return entries;
    } catch (err) {
      console.error("Failed to scrape YouTube HTML", err);
      return [];
    }
  };

  try {
    const rssUrl = `https://www.youtube.com/feeds/videos.xml?channel_id=${channelId}`;
    const res = await fetch(rssUrl, {
      headers: { "user-agent": "Mozilla/5.0 (compatible; D1Worker/1.0)" },
    });
    if (!res.ok) throw new Error(`RSS status ${res.status}`);
    const xml = await res.text();
    const entries: {
      title: string;
      url: string;
      publishedAt: string;
      thumbnailUrl?: string | null;
      videoId?: string | null;
    }[] = [];
    const entryRegex = /<entry>([\s\S]*?)<\/entry>/g;
    let match: RegExpExecArray | null;
    while ((match = entryRegex.exec(xml)) && entries.length < 3) {
      const block = match[1];
      const title = decodeHtmlEntities(block.match(/<title>([^<]+)<\/title>/)?.[1] || "Video");
      const link = block.match(/<link[^>]+href="([^"]+)"/)?.[1] || "";
      const publishedAt = block.match(/<published>([^<]+)<\/published>/)?.[1] || "";
      const thumb =
        block.match(/media:thumbnail[^>]+url="([^"]+)"/)?.[1] ||
        block.match(/<media:thumbnail url='([^']+)'/i)?.[1] ||
        null;
      const videoId = block.match(/<yt:videoId>([^<]+)<\/yt:videoId>/)?.[1] || null;
      entries.push({
        title,
        url: videoId ? `https://www.youtube.com/watch?v=${videoId}` : link,
        publishedAt,
        thumbnailUrl: thumb,
        videoId,
      });
    }
    if (entries.length > 0) {
      await saveCache(entries);
      return entries;
    }
    const htmlEntries = await fetchFromHtml();
    if (htmlEntries.length > 0) {
      await saveCache(htmlEntries);
      return htmlEntries;
    }
    // empty feed: fallback to cache
    const cached = await loadCache();
    if (cached.length > 0) return cached;
    await saveCache(demoVideos);
    return demoVideos;
  } catch (e) {
    console.error("Failed to fetch YouTube RSS", e);
    const htmlEntries = await fetchFromHtml();
    if (htmlEntries.length > 0) {
      await saveCache(htmlEntries);
      return htmlEntries;
    }
    const cached = await loadCache();
    if (cached.length > 0) return cached;
    await saveCache(demoVideos);
    return demoVideos;
  }
}

async function fetchYoutubeMetricsFromComposio(env: Env): Promise<YoutubeMetrics | null> {
  if (!env.COMPOSIO_API_KEY || !env.COMPOSIO_YT_ACCOUNT_ID || !env.COMPOSIO_YT_ENTITY_ID) {
    console.warn("Composio YouTube not configured, falling back to demo");
    return null;
  }

  const baseUrl = "https://backend.composio.dev/api/v3/tools/execute";

  const callTool = async (tool: string, args: Record<string, any>) => {
    const payload = {
      connected_account_id: env.COMPOSIO_YT_ACCOUNT_ID,
      entity_id: env.COMPOSIO_YT_ENTITY_ID,
      arguments: args,
    };

    const res = await fetch(`${baseUrl}/${tool}`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-api-key": env.COMPOSIO_API_KEY!,
      },
      body: JSON.stringify(payload),
    });

    const text = await res.text();

    if (!res.ok) {
      throw new Error(`Composio tool ${tool} failed: ${res.status} ${text}`);
    }
    try {
      return JSON.parse(text);
    } catch (e) {
      throw new Error(`Composio tool ${tool} invalid JSON: ${text}`);
    }
  };

  try {
    const handle = env.COMPOSIO_YT_CHANNEL_HANDLE || "irina2755";

    const resolveChannelId = async (): Promise<string | null> => {
      try {
        const primary = await callTool("YOUTUBE_GET_CHANNEL_ID_BY_HANDLE", {
          channel_handle: `@${handle.replace(/^@/, "")}`,
        });
        console.log("YT handle->id primary", JSON.stringify(primary, null, 2));
        const found =
          primary?.data?.id ||
          primary?.data?.channelId ||
          primary?.data?.channel_id ||
          primary?.data?.items?.[0]?.id ||
          primary?.data?.items?.[0]?.channelId;
        if (typeof found === "string" && found.length > 0) return found;
      } catch (err) {
        console.error("Handle->ID lookup failed", err);
      }
      try {
        const alt = await callTool("YOUTUBE_GET_CHANNEL_ID_BY_HANDLE", {
          channel_handle: handle.replace(/^@/, ""),
        });
        console.log("YT handle->id alt", JSON.stringify(alt, null, 2));
        const found =
          alt?.data?.id ||
          alt?.data?.channelId ||
          alt?.data?.channel_id ||
          alt?.data?.items?.[0]?.id ||
          alt?.data?.items?.[0]?.channelId;
        if (typeof found === "string" && found.length > 0) return found;
      } catch (err) {
        console.error("Handle->ID alt lookup failed", err);
      }
      return env.COMPOSIO_YT_CHANNEL_ID ?? null;
    };

    const channelId = await resolveChannelId();
    if (!channelId) return null;

    const resp = await callTool("YOUTUBE_GET_CHANNEL_STATISTICS", {
      id: channelId,
      part: "statistics",
    });

    let stats =
      resp?.data?.items?.[0]?.statistics ||
      resp?.data?.channels?.[0]?.statistics;

    // Если первый вызов ничего не вернул, пробуем альтернативный вариант параметров
    if (!stats) {
      try {
        const alt = await callTool("YOUTUBE_GET_CHANNEL_STATISTICS", {
          channelId,
          part: "statistics",
        });
        stats = alt?.data?.items?.[0]?.statistics;
      } catch (e) {
        console.error("Alt YT stats fetch failed", e);
      }
    }

    let topVideo: YoutubeMetrics["topVideo"] = null;
    let videoViewsFromDetails = 0;
    try {
      const videoId = TOP_VIDEO_ID;
      const videoResp = await callTool("YOUTUBE_VIDEO_DETAILS", {
        id: videoId,
        part: "snippet,statistics",
      });
      const item = videoResp?.data?.items?.[0] || videoResp?.data?.videos?.[0] || videoResp?.items?.[0];
      const statsVideo = item?.statistics;
      const views =
        statsVideo?.viewCount !== undefined && statsVideo?.viewCount !== null
          ? parseNumber(statsVideo.viewCount, 0)
          : undefined;
      if (views !== undefined) videoViewsFromDetails = Math.max(videoViewsFromDetails, views);
    } catch (e) {
      console.error("Video details fetch failed via v3 tool", e);
    }

    if (!stats) {
      console.warn("YT stats empty after tool call", JSON.stringify(resp));
      return null;
    }

    const allTimeViewsRaw = parseNumber(stats.viewCount, 0);
    const allTimeViews = videoViewsFromDetails > 0 ? videoViewsFromDetails : allTimeViewsRaw > 0 ? allTimeViewsRaw : 0;
    const subscribers = parseNumber(stats.subscriberCount, 0);
    const videoCount = parseNumber(stats.videoCount, 0);

    return {
      viewsToday: 0, // вычислим дельты при записи в БД
      views7d: 0,
      views30d: 0,
      allTimeViews,
      subscribers,
      newVideos30d: 0, // вычислим дельты при записи в БД (по videoCount)
      videoCount,
      topVideo,
    };
  } catch (err) {
    console.error("Failed to load YouTube metrics from Composio", err);
    return null;
  }
}

// Загрузка метрик YouTube через Composio (заготовка, нужно дополнить реальными вызовами)
// Демо-обновление YouTube данных
async function refreshYoutubeDemo(env: Env): Promise<void> {
  console.log("Running YouTube demo refresh (DEMO_MODE only)");
  await env.DB
    .prepare(
      `INSERT INTO youtube_daily
         (updated_at, views_today, views_7d, views_30d, views_all_time, subscribers, new_videos_30d)
       VALUES (?, ?, ?, ?, ?, ?, ?)`
    )
    .bind(new Date().toISOString(), 1234, 8567, 32450, 1200000, 182000, 4)
    .run();
}

// Запись снимка YouTube на основе тоталов и исторических значений
async function upsertYoutubeSnapshot(env: Env, metrics: YoutubeMetrics) {
  try {
    await env.DB.prepare("ALTER TABLE youtube_daily ADD COLUMN videos_total INTEGER").run();
  } catch (_) {
    // колонка уже есть или миграция не требуется
  }
  try {
    await env.DB.prepare("ALTER TABLE youtube_daily ADD COLUMN top_video_title TEXT").run();
  } catch (_) {}
  try {
    await env.DB.prepare("ALTER TABLE youtube_daily ADD COLUMN top_video_views INTEGER").run();
  } catch (_) {}
  try {
    await env.DB.prepare("ALTER TABLE youtube_daily ADD COLUMN top_video_url TEXT").run();
  } catch (_) {}
  try {
    await env.DB.prepare("ALTER TABLE youtube_daily ADD COLUMN top_video_published_at TEXT").run();
  } catch (_) {}

  const nowIso = new Date().toISOString();

  const { results: prevRows } = await env.DB.prepare(
    "SELECT updated_at, views_all_time, subscribers, new_videos_30d, videos_total FROM youtube_daily ORDER BY updated_at DESC"
  ).all<{
    updated_at: string;
    views_all_time: number;
    subscribers: number;
    new_videos_30d?: number | null;
    videos_total?: number | null;
  }>();

  const prev = prevRows?.[0];
  const prev7 = prevRows?.find((r) => r.updated_at <= daysAgoIso(7));
  const prev30 = prevRows?.find((r) => r.updated_at <= daysAgoIso(30));

  const allTimeViews = metrics.allTimeViews;
  const viewsToday = prev ? Math.max(0, allTimeViews - parseNumber(prev.views_all_time, 0)) : 0;
  const views7d = prev7 ? Math.max(0, allTimeViews - parseNumber(prev7.views_all_time, 0)) : allTimeViews;
  const views30d = prev30 ? Math.max(0, allTimeViews - parseNumber(prev30.views_all_time, 0)) : allTimeViews;

  const videoCount = metrics.videoCount;
  const prevVideos = parseNumber(prev?.videos_total ?? prev?.new_videos_30d, 0);
  const prevVideos30 = parseNumber(prev30?.videos_total ?? prev30?.new_videos_30d, prevVideos);
  const newVideos30d = Math.max(0, videoCount - prevVideos30);

  await env.DB
    .prepare(
      `INSERT INTO youtube_daily
         (updated_at, views_today, views_7d, views_30d, views_all_time, subscribers, new_videos_30d, top_video_title, top_video_views, top_video_url, top_video_published_at)
       VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)`
    )
    .bind(
      nowIso,
      viewsToday,
      views7d,
      views30d,
      allTimeViews,
      metrics.subscribers,
      newVideos30d,
      metrics.topVideo?.title ?? null,
      metrics.topVideo ? parseNumber(metrics.topVideo.views, 0) : null,
      metrics.topVideo?.url ?? null,
      metrics.topVideo?.publishedAt ?? null
    )
    .run();

  try {
    await env.DB.prepare("UPDATE youtube_daily SET videos_total = ? WHERE updated_at = ?").bind(videoCount, nowIso).run();
  } catch (_) {
    // если колонки нет - пропускаем
  }
}

async function refreshSalesFromSheets(env: Env): Promise<void> {
  const spreadsheetId = env.SALES_SHEET_ID || "1AgeLC2KWxoxPPfhBsLaOdcSnkr4iB_VVuwveWt5yGOE";
  const range = env.SALES_SHEET_RANGE || "Лист1!A:F";

  const connectedAccountId = env.COMPOSIO_SHEETS_ACCOUNT_ID;
  const entityId = env.COMPOSIO_SHEETS_ENTITY_ID;

  if (!env.COMPOSIO_API_KEY || !connectedAccountId || !spreadsheetId) {
    console.warn("Composio Sheets not configured, falling back to demo sales");
    const existing = await env.DB.prepare("SELECT 1 AS ok FROM sales_daily LIMIT 1").first();
    if (!existing) await refreshSalesDemo(env);
    return;
  }

  try {
    const res = await fetch("https://backend.composio.dev/api/v3/tools/execute/GOOGLESHEETS_BATCH_GET", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-api-key": env.COMPOSIO_API_KEY,
      },
      body: JSON.stringify({
        connected_account_id: connectedAccountId,
        entity_id: entityId,
        arguments: {
          spreadsheet_id: spreadsheetId,
          ranges: [range],
        },
      }),
    });

    const text = await res.text();
    if (!res.ok) {
      console.error("Sheets fetch failed:", res.status, text.slice(0, 800));
      const existing = await env.DB.prepare("SELECT 1 AS ok FROM sales_daily LIMIT 1").first();
      if (!existing) await refreshSalesDemo(env);
      return;
    }

    let payload: any = null;
    try {
      payload = JSON.parse(text);
    } catch (e) {
      console.error("Sheets response is not JSON:", text.slice(0, 800));
      const existing = await env.DB.prepare("SELECT 1 AS ok FROM sales_daily LIMIT 1").first();
      if (!existing) await refreshSalesDemo(env);
      return;
    }

    if (payload?.successful === false) {
      console.error("Sheets tool unsuccessful:", String(payload?.error ?? "unknown error"));
      const existing = await env.DB.prepare("SELECT 1 AS ok FROM sales_daily LIMIT 1").first();
      if (!existing) await refreshSalesDemo(env);
      return;
    }

    const spreadsheetData = payload?.data?.spreadsheet_data;
    const valueRanges =
      spreadsheetData?.valueRanges ??
      spreadsheetData?.value_ranges ??
      payload?.data?.valueRanges ??
      payload?.data?.value_ranges;
    const values =
      (Array.isArray(valueRanges) ? valueRanges?.[0]?.values : valueRanges?.values) ||
      spreadsheetData?.values ||
      payload?.data?.values ||
      [];

    if (!Array.isArray(values) || values.length === 0) {
      console.warn(
        "Sheets returned empty values, payload snippet:",
        JSON.stringify(payload).slice(0, 1500)
      );
      const existing = await env.DB.prepare("SELECT 1 AS ok FROM sales_daily LIMIT 1").first();
      if (!existing) await refreshSalesDemo(env);
      return;
    }

    console.log("✅ Sheets data received:", values.length, "rows");
    console.log("📊 First row:", JSON.stringify(values[0]));
    console.log("📊 Last row:", JSON.stringify(values[values.length - 1]));
    console.log("🔍 Range used:", range);

    const byDate: Record<
      string,
      { totalSales: number; revenueCents: number; profitCents: number }
    > = {};

    for (const row of values) {
      if (!Array.isArray(row)) continue;
      const date = String(row[0] ?? "").trim();
      const status = String(row[5] ?? "").trim();
      if (!status) continue;
      if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) continue;

      const priceCents = moneyStringToCents(String(row[3] ?? ""));
      const profitCents = moneyStringToCents(String(row[4] ?? ""));

      if (!byDate[date]) byDate[date] = { totalSales: 0, revenueCents: 0, profitCents: 0 };
      byDate[date].totalSales += 1;
      byDate[date].revenueCents += priceCents;
      byDate[date].profitCents += profitCents;
    }

    const dates = Object.keys(byDate).sort();
    console.log("📅 Processed dates:", dates.length);
    console.log("📅 Last 5 dates:", dates.slice(-5));
    
    if (dates.length === 0) {
      console.warn("No valid sales rows found in Sheets, falling back to demo sales");
      const existing = await env.DB.prepare("SELECT 1 AS ok FROM sales_daily LIMIT 1").first();
      if (!existing) await refreshSalesDemo(env);
      return;
    }

    await env.DB.prepare("DELETE FROM sales_daily").run();
    const insert = env.DB.prepare(
      `INSERT INTO sales_daily (date, total_sales, total_revenue_cents, total_profit_cents, avg_profit_cents)
       VALUES (?, ?, ?, ?, ?)`
    );

    for (const d of dates) {
      const agg = byDate[d];
      const avgProfitCents = agg.totalSales > 0 ? Math.round(agg.profitCents / agg.totalSales) : 0;
      await insert.bind(d, agg.totalSales, agg.revenueCents, agg.profitCents, avgProfitCents).run();
    }
  } catch (err) {
    console.error("Failed to refresh sales from Sheets, falling back to demo", err);
    const existing = await env.DB.prepare("SELECT 1 AS ok FROM sales_daily LIMIT 1").first();
    if (!existing) await refreshSalesDemo(env);
  }
}

// Демо-обновление Sales данных
async function refreshSalesDemo(env: Env): Promise<void> {
  await env.DB.prepare("DELETE FROM sales_daily").run();

  await env.DB
    .prepare(
      `INSERT INTO sales_daily
         (date, total_sales, total_revenue_cents, total_profit_cents, avg_profit_cents)
       VALUES (?, ?, ?, ?, ?)`
    )
    .bind(
      "2025-03-15",
      147, // total_sales
      368000, // total_revenue_cents (3680.00 * 100)
      124000, // total_profit_cents (1240.00 * 100)
      844 // avg_profit_cents (8.44 * 100)
    )
    .run();
}

// Обновление demo-постов Telegram в базе
async function refreshTelegram(env: Env) {
  const slug = env.TELEGRAM_CHANNEL_SLUG || "my_channel";

  const demoPosts = [
    {
      message_id: "451",
      text: "Новый ролик! Ссылка в описании канала 🤍",
      published_at: "2025-03-15T07:10:00Z",
      message_url: `https://t.me/${slug}/451`,
    },
    {
      message_id: "448",
      text: "Еженедельный дайджест: лучшие моменты за неделю ✨",
      published_at: "2025-03-14T12:05:00Z",
      message_url: `https://t.me/${slug}/448`,
    },
    {
      message_id: "443",
      text: "Немного закулисья со вчерашней съёмки 🎬",
      published_at: "2025-03-12T18:45:00Z",
      message_url: `https://t.me/${slug}/443`,
    },
  ];

  const parseTelegramPosts = (html: string, limit = TELEGRAM_PARSE_LIMIT) => {
    const result: { id: string; text: string; publishedAt: string }[] = [];
    const postRegex = /data-post="[^/]+\/(\d+)"[\s\S]*?datetime="([^"]+)"[\s\S]*?class="tgme_widget_message_text[^"]*"[^>]*>([\s\S]*?)<\/div>/g;
    let match: RegExpExecArray | null;
    while ((match = postRegex.exec(html)) && result.length < limit) {
      const [, id, datetime, rawText] = match;
      const text = rawText
        .replace(/<br\s*\/?>/gi, "\n")
        .replace(/<[^>]+>/g, " ")
        .replace(/\s+/g, " ")
        .trim();
      result.push({ id, text, publishedAt: datetime });
    }
    return result;
  };

  let posts = demoPosts;
  try {
    const res = await fetch(`https://t.me/s/${slug}`, { method: "GET" });
    if (res.ok) {
      const html = await res.text();
      const parsed = parseTelegramPosts(html);
      if (parsed.length > 0) {
        posts = parsed.map((p) => ({
          message_id: p.id,
          text: p.text || "Без текста",
          published_at: p.publishedAt,
          message_url: `https://t.me/${slug}/${p.id}`,
        }));
      }
    }
  } catch (err) {
    console.error("Failed to fetch Telegram HTML, fallback to demo", err);
  }

  const cutoff = new Date();
  cutoff.setUTCDate(cutoff.getUTCDate() - TELEGRAM_RETENTION_DAYS);
  const cutoffIso = cutoff.toISOString();
  await env.DB.prepare("DELETE FROM telegram_posts WHERE channel_slug = ? AND published_at < ?").bind(slug, cutoffIso).run();

  const insert = env.DB.prepare(
    "INSERT OR REPLACE INTO telegram_posts (channel_slug, message_id, text, published_at, message_url) VALUES (?, ?, ?, ?, ?)"
  );

  for (const post of posts.slice(0, TELEGRAM_MAX_POSTS)) {
    await insert.bind(slug, post.message_id, post.text, post.published_at, post.message_url).run();
  }
}
