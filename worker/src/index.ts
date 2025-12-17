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
};

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

      await refreshSalesDemo(env);
      await refreshTelegram(env);
      return jsonResponse({
        ok: true,
        message: "Refresh demo data updated in D1",
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
           videos_total
         FROM youtube_daily
         ORDER BY updated_at DESC
         LIMIT 1`
      ).first();
    } catch (err) {
      // если нет колонки videos_total — добавим и попробуем снова
      try {
        await env.DB.prepare("ALTER TABLE youtube_daily ADD COLUMN videos_total INTEGER").run();
        ytRow = await env.DB.prepare(
          `SELECT
             updated_at,
             views_today,
             views_7d,
             views_30d,
             views_all_time,
             subscribers,
             new_videos_30d,
             videos_total
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

    // --- YouTube метрики и график ---
    let youtubeMetrics;
    let youtubeChart;

    if (ytRow) {
      youtubeMetrics = {
        viewsToday: ytRow.views_today ?? 0,
        views7d: ytRow.views_7d ?? 0,
        views30d: ytRow.views_30d ?? 0,
        allTimeViews: ytRow.views_all_time ?? 0,
        newVideos30d: ytRow.new_videos_30d ?? 0,
        subscribers: ytRow.subscribers ?? 0,
      };
      // временно используем демо-график, пока не строим из истории
      youtubeChart = demoPayload.youtube.chart;
    } else {
      youtubeMetrics = demoPayload.youtube.metrics;
      youtubeChart = demoPayload.youtube.chart;
    }

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
      },
      sales: {
        metrics: salesMetrics,
        chart: salesChart,
      },
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

function parseNumber(value: any, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function daysAgoIso(days: number) {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() - days);
  return d.toISOString();
}

async function fetchYoutubeMetricsFromComposio(env: Env): Promise<YoutubeMetrics | null> {
  if (!env.COMPOSIO_API_KEY || !env.COMPOSIO_YT_ACCOUNT_ID || !env.COMPOSIO_YT_ENTITY_ID) {
    console.warn("Composio YouTube not configured, falling back to demo");
    return null;
  }

  const baseUrl = "https://backend.composio.dev/api/v3/tools/execute";

  const callTool = async (tool: string, args: Record<string, any>) => {
    const res = await fetch(`${baseUrl}/${tool}`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-api-key": env.COMPOSIO_API_KEY!,
      },
      body: JSON.stringify({
        connected_account_id: env.COMPOSIO_YT_ACCOUNT_ID,
        entity_id: env.COMPOSIO_YT_ENTITY_ID,
        arguments: args,
      }),
    });
    if (!res.ok) {
      const text = await res.text();
      throw new Error(`Composio tool ${tool} failed: ${res.status} ${text}`);
    }
    return res.json();
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
    try {
      const search = await callTool("YOUTUBE_SEARCH_LIST", {
        channelId,
        order: "viewCount",
        type: "video",
        part: "snippet",
        maxResults: 1,
      });
      const first = search?.data?.items?.[0];
      const videoId = first?.id?.videoId || first?.id || first?.videoId || first?.video_id;
      const title = first?.snippet?.title;
      const publishedAt = first?.snippet?.publishedAt;
      let views = 0;

      if (videoId) {
        try {
          const videoStats = await callTool("YOUTUBE_VIDEOS_LIST", {
            id: videoId,
            part: "statistics,snippet",
          });
          const vstats =
            videoStats?.data?.items?.[0]?.statistics || videoStats?.data?.videos?.[0]?.statistics;
          views = parseNumber(vstats?.viewCount, 0);
        } catch (err) {
          console.error("Top video stats fetch failed", err);
        }
      }

      if (title && videoId) {
        topVideo = {
          title,
          views,
          publishedAt,
          url: `https://youtube.com/watch?v=${videoId}`,
          videoId,
        };
      }
    } catch (err) {
      console.error("Top video lookup failed", err);
    }

    if (!stats) {
      console.warn("YT stats empty after tool call", JSON.stringify(resp));
      return null;
    }

    const allTimeViews = parseNumber(stats.viewCount, 0);
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
