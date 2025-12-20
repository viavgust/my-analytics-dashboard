// @ts-nocheck
// Тип окружения: сюда Cloudflare "привяжет" нашу D1-базу с binding = "DB"
export interface Env {
  DB: D1Database;
  TELEGRAM_CHANNEL_SLUG?: string;
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

// Главный обработчик Worker-а
export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);

    // CORS preflight
    if (request.method === "OPTIONS") {
      if (url.pathname === "/api/dashboard" || url.pathname === "/api/refresh") {
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

    // POST /api/refresh — пока заглушка
    if ((request.method === "POST" || request.method === "GET") && url.pathname === "/api/refresh") {
      console.log("Refresh called at", new Date().toISOString());
      console.log("Has COMPOSIO_API_KEY:", !!env.COMPOSIO_API_KEY);
      const ytMetrics = await fetchYoutubeMetricsFromComposio(env);
      if (ytMetrics) {
        await upsertYoutubeSnapshot(env, ytMetrics);
      } else {
        await refreshYoutubeDemo(env);
      }

      await refreshSalesFromSheets(env);
      await refreshTelegram(env);
      await refreshCalendar(env);
      return jsonResponse({
        ok: true,
        message: "Refresh completed (telegram/youtube/sales/calendar)",
      });
    }

    // Всё остальное — 404
    return new Response("Not found", { status: 404 });
  },
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
      "SELECT * FROM sales_daily ORDER BY date DESC LIMIT 30"
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
      // временно используем демо-график, пока не строим из истории
      youtubeChart = demoPayload.youtube.chart;
    } else {
      youtubeMetrics = demoPayload.youtube.metrics;
      youtubeChart = demoPayload.youtube.chart;
      youtubeTopVideo = demoPayload.youtube.topVideo ?? null;
    }
    const youtubeLatestVideos =
      latestVideos.length > 0
        ? latestVideos
        : demoPayload.youtube.latestVideos ?? [];

    // --- Sales / eBay метрики и график ---

    // Вспомогательная функция: сумма по полю
    const sumField = (rows: any[], field: string): number =>
      rows.reduce((sum, row) => sum + (row[field] ?? 0), 0);

    let salesMetrics;
    let salesChart;

    if (salesRows.length) {
      const latest = salesRows[0] as any;
      const last7 = salesRows.slice(0, 7);
      const last30 = salesRows.slice(0, 30);

      const totalSales7d = sumField(last7, "total_sales");
      const totalRevenue7dCents = sumField(last7, "total_revenue_cents");
      const totalProfit7dCents = sumField(last7, "total_profit_cents");

      const totalSales30d = sumField(last30, "total_sales");
      const totalRevenue30dCents = sumField(last30, "total_revenue_cents");
      const totalProfit30dCents = sumField(last30, "total_profit_cents");

      // Для карточки можно взять агрегаты за 30 дней
      const totalSales = totalSales30d;
      const totalRevenue = centsToDollars(totalRevenue30dCents);
      const totalProfit = centsToDollars(totalProfit30dCents);
      const avgProfit =
        totalSales > 0 ? totalProfit / totalSales : 0;

      salesMetrics = {
        totalSales,
        totalRevenue,
        totalProfit,
        avgProfit,
      };

      const salesPoints = salesRows
        .slice()
        .reverse()
        .map((row: any) => ({
          label: row.date,
          revenue: centsToDollars(row.total_revenue_cents ?? 0),
        }));

      salesChart = {
        granularity: "day",
        points: salesPoints,
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
      channel: "my_channel",
      posts: [
        {
          messageId: "451",
          text: "Новый ролик! Ссылка в описании канала 🤍",
          publishedAt: "2025-03-15T07:10:00Z",
        },
        {
          messageId: "448",
          text: "Еженедельный дайджест: лучшие моменты за неделю ✨",
          publishedAt: "2025-03-14T12:05:00Z",
        },
        {
          messageId: "443",
          text: "Немного закулисья со вчерашней съёмки 🎬",
          publishedAt: "2025-03-12T18:45:00Z",
        },
      ],
    },
    youtube: {
      metrics: {
        viewsToday: 1234,
        views7d: 8567,
        views30d: 32450,
        allTimeViews: 1200000,
        newVideos30d: 4,
        subscribers: 182000,
      },
      chart: {
        granularity: "month",
        points: [
          { label: "Oct", views: 12000 },
          { label: "Nov", views: 18000 },
          { label: "Dec", views: 15000 },
          { label: "Jan", views: 22000 },
          { label: "Feb", views: 28000 },
          { label: "Mar", views: 32450 },
        ],
      },
      latestVideos: [
        {
          title: "Travel guide: hidden gems in the city",
          url: "https://youtube.com/watch?v=demo1",
          publishedAt: "2025-03-15T10:00:00Z",
          thumbnailUrl: "https://i.ytimg.com/vi/demo1/hqdefault.jpg",
          videoId: "demo1",
        },
        {
          title: "Weekend getaway highlights",
          url: "https://youtube.com/watch?v=demo2",
          publishedAt: "2025-03-14T08:30:00Z",
          thumbnailUrl: "https://i.ytimg.com/vi/demo2/hqdefault.jpg",
          videoId: "demo2",
        },
        {
          title: "Street food tour",
          url: "https://youtube.com/watch?v=demo3",
          publishedAt: "2025-03-12T18:00:00Z",
          thumbnailUrl: "https://i.ytimg.com/vi/demo3/hqdefault.jpg",
          videoId: "demo3",
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
function jsonResponse(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
    },
  });
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
      "SELECT summary, start_time, end_time, html_link FROM calendar_events WHERE start_time >= ? ORDER BY start_time ASC LIMIT 5"
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

  try {
    const rssUrl = `https://www.youtube.com/feeds/videos.xml?channel_id=${channelId}`;
    const res = await fetch(rssUrl, {
      headers: { "user-agent": "Mozilla/5.0 (compatible; D1Worker/1.0)" },
    });
    if (!res.ok) return [];
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
        url: link,
        publishedAt,
        thumbnailUrl: thumb,
        videoId,
      });
    }
    return entries;
  } catch (e) {
    console.error("Failed to fetch YouTube RSS", e);
    return [];
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

  const parseTelegramPosts = (html: string) => {
    const result: { id: string; text: string; publishedAt: string }[] = [];
    const postRegex = /data-post="[^/]+\/(\d+)"[\s\S]*?datetime="([^"]+)"[\s\S]*?class="tgme_widget_message_text[^"]*"[^>]*>([\s\S]*?)<\/div>/g;
    let match: RegExpExecArray | null;
    while ((match = postRegex.exec(html)) && result.length < 3) {
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

  await env.DB.prepare("DELETE FROM telegram_posts WHERE channel_slug = ?").bind(slug).run();

  const insert = env.DB.prepare(
    "INSERT INTO telegram_posts (channel_slug, message_id, text, published_at, message_url) VALUES (?, ?, ?, ?, ?)"
  );

  for (const post of posts.slice(0, 3)) {
    await insert.bind(slug, post.message_id, post.text, post.published_at, post.message_url).run();
  }
}
