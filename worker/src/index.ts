// @ts-nocheck
// Тип окружения: сюда Cloudflare "привяжет" нашу D1-базу с binding = "DB"
export interface Env {
  DB: D1Database;
  TELEGRAM_CHANNEL_SLUG?: string;
  COMPOSIO_API_KEY?: string;
  COMPOSIO_YT_AUTH_CONFIG_ID?: string;
  COMPOSIO_YT_ACCOUNT_ID?: string;
}

type YoutubeMetrics = {
  viewsToday: number;
  views7d: number;
  views30d: number;
  allTimeViews: number;
  subscribers: number;
  newVideos30d: number;
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
        const { viewsToday, views7d, views30d, allTimeViews, subscribers, newVideos30d } = ytMetrics;
        await env.DB.prepare(`DELETE FROM youtube_daily`).run();
        await env.DB.prepare(
          `INSERT INTO youtube_daily(
             updated_at,
             views_today,
             views_7d,
             views_30d,
             views_all_time,
             subscribers,
             new_videos_30d
           ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)`
        )
          .bind(new Date().toISOString(), viewsToday, views7d, views30d, allTimeViews, subscribers, newVideos30d)
          .run();
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
    const ytRow = await env.DB.prepare(
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
    ).first<{
      updated_at: string;
      views_today: number;
      views_7d: number;
      views_30d: number;
      views_all_time: number;
      subscribers: number;
      new_videos_30d: number;
    }>();

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

// Загрузка метрик YouTube через Composio (заготовка, нужно дополнить реальными вызовами)
async function fetchYoutubeMetricsFromComposio(env: Env): Promise<YoutubeMetrics | null> {
  if (!env.COMPOSIO_API_KEY || !env.COMPOSIO_YT_AUTH_CONFIG_ID || !env.COMPOSIO_YT_ACCOUNT_ID) {
    console.warn("Composio YouTube not configured, falling back to demo");
    return null;
  }

  try {
    // TODO: взять точный эндпоинт и формат тела запроса из официальной документации Composio.
    // Общая идея:
    // 1) Вызвать YouTube-tool "list channel videos" (или эквивалент) через Composio
    //    с полями snippet + statistics, используя auth_config_id и account_id.
    // 2) Для каждого видео взять:
    //    - publishedAt
    //    - statistics.viewCount
    // 3) Посчитать агрегаты:
    //    - allTimeViews = сумма viewCount по всем видео
    //    - views30d = сумма viewCount по видео, опубликованным за последние 30 дней
    //    - views7d = сумма viewCount по видео, опубликованным за последние 7 дней
    //    - viewsToday = сумма viewCount по видео, опубликованным "сегодня" (по дате, не по часу)
    //    - subscribers и newVideos30d можно взять:
    //        subscribers — из "get_channel_statistics"
    //        newVideos30d — количество видео с publishedAt за последние 30 дней
    //
    // Внутри этой функции используй fetch к HTTP-API Composio или их JS-SDK
    // (как удобнее для Cloudflare Worker), с Authorization: Bearer env.COMPOSIO_API_KEY.

    return {
      viewsToday: 0,
      views7d: 0,
      views30d: 0,
      allTimeViews: 0,
      subscribers: 0,
      newVideos30d: 0,
    };
  } catch (err) {
    console.error("Failed to load YouTube metrics from Composio", err);
    return null;
  }
}

// Демо-обновление YouTube данных
async function refreshYoutubeDemo(env: Env): Promise<void> {
  await env.DB.prepare("DELETE FROM youtube_daily").run();

  await env.DB
    .prepare(
      `INSERT INTO youtube_daily
         (updated_at, views_today, views_7d, views_30d, views_all_time, subscribers, new_videos_30d)
       VALUES (?, ?, ?, ?, ?, ?, ?)`
    )
    .bind(
      "2025-03-15T00:00:00.000Z",
      1234, // views_today
      8567, // views_7d (из демо)
      32450, // views_30d (из демо)
      1200000, // views_all_time
      182000, // subscribers
      4 // new_videos_30d
    )
    .run();
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

  await env.DB.prepare("DELETE FROM telegram_posts WHERE channel_slug = ?").bind(slug).run();

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

  const insert = env.DB.prepare(
    "INSERT INTO telegram_posts (channel_slug, message_id, text, published_at, message_url) VALUES (?, ?, ?, ?, ?)"
  );

  for (const post of demoPosts) {
    await insert.bind(slug, post.message_id, post.text, post.published_at, post.message_url).run();
  }
}
