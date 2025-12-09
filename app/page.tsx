"use client";

import { useMemo, useState } from "react";

type YoutubeStat = {
  label: string;
  value: number;
  suffix?: string;
  delta: number;
};

type YoutubeVideo = {
  title: string;
  views: number;
  ctr: number;
  retention: number;
};

type SalesRow = {
  channel: string;
  region: string;
  orders: number;
  revenue: number;
  avgOrder: number;
};

type ThirdCard = {
  title: string;
  body: string;
  badge: string;
};

const initialYoutubeStats: YoutubeStat[] = [
  { label: "Views", value: 128_400, delta: 12.4 },
  { label: "Watch time", value: 6_420, suffix: "h", delta: 8.1 },
  { label: "New subscribers", value: 1_840, delta: 6.3 },
];

const initialYoutubeVideos: YoutubeVideo[] = [
  { title: "Product walkthrough: v2.4", views: 48_200, ctr: 6.1, retention: 56 },
  { title: "Feature deep dive: automation", views: 36_800, ctr: 5.4, retention: 61 },
  { title: "Customer story: Northwind", views: 28_900, ctr: 7.2, retention: 53 },
];

const initialSales: SalesRow[] = [
  { channel: "Website", region: "North America", orders: 182, revenue: 28_600, avgOrder: 157 },
  { channel: "Resellers", region: "EMEA", orders: 96, revenue: 19_450, avgOrder: 202 },
  { channel: "Events", region: "APAC", orders: 64, revenue: 12_080, avgOrder: 189 },
];

const initialThirdSource: ThirdCard[] = [
  { title: "NPS pulse", body: "Third source placeholder with a weekly check-in score and quick actions.", badge: "3rd source" },
  { title: "Churn watch", body: "Flagged accounts with slipping engagement. Replace with real API later.", badge: "3rd source" },
  { title: "Roadmap votes", body: "Top-voted ideas to align with releases. Data mocked for now.", badge: "3rd source" },
];

const jitter = (value: number, variance = 0.16) =>
  Math.max(0, Math.round(value * (1 + (Math.random() - 0.5) * variance)));

const jitterDecimal = (value: number, variance = 0.12) =>
  Math.max(0, parseFloat((value * (1 + (Math.random() - 0.5) * variance)).toFixed(1)));

const formatNumber = (value: number) => value.toLocaleString("en-US");

const formatPercent = (value: number) => `${value > 0 ? "+" : ""}${value.toFixed(1)}%`;

export default function Home() {
  const [youtubeStats, setYoutubeStats] = useState<YoutubeStat[]>(initialYoutubeStats);
  const [youtubeVideos, setYoutubeVideos] = useState<YoutubeVideo[]>(initialYoutubeVideos);
  const [sales, setSales] = useState<SalesRow[]>(initialSales);
  const [thirdCards, setThirdCards] = useState<ThirdCard[]>(initialThirdSource);
  const [lastRefreshed, setLastRefreshed] = useState<Date>(new Date());

  const totalRevenue = useMemo(
    () => sales.reduce((sum, row) => sum + row.revenue, 0),
    [sales],
  );

  const handleRefresh = () => {
    setYoutubeStats((stats) =>
      stats.map((stat) => ({
        ...stat,
        value: stat.suffix ? jitterDecimal(stat.value) : jitter(stat.value),
        delta: jitterDecimal(stat.delta, 0.4),
      })),
    );

    setYoutubeVideos((videos) =>
      videos.map((video) => ({
        ...video,
        views: jitter(video.views),
        ctr: jitterDecimal(video.ctr, 0.18),
        retention: jitter(video.retention),
      })),
    );

    setSales((rows) =>
      rows.map((row) => ({
        ...row,
        orders: jitter(row.orders, 0.2),
        revenue: jitter(row.revenue, 0.2),
        avgOrder: jitter(row.avgOrder, 0.12),
      })),
    );

    setThirdCards((cards) =>
      cards.map((card, index) => ({
        ...card,
        body:
          index === 0
            ? "Third source placeholder refreshed with a new pulse snapshot."
            : card.body,
      })),
    );

    setLastRefreshed(new Date());
  };

  const formattedTime = useMemo(
    () =>
      lastRefreshed.toLocaleString("ru-RU", {
        dateStyle: "medium",
        timeStyle: "short",
      }),
    [lastRefreshed],
  );

  return (
    <div className="relative min-h-screen overflow-hidden bg-amber-50 text-stone-900">
      <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_16%_18%,rgba(255,210,170,0.55),transparent_35%),radial-gradient(circle_at_82%_10%,rgba(255,183,197,0.35),transparent_32%),radial-gradient(circle_at_35%_85%,rgba(255,222,193,0.35),transparent_38%),linear-gradient(150deg,#fffaf5,#fff)]" />
      <div className="pointer-events-none absolute inset-0 bg-[linear-gradient(120deg,rgba(255,255,255,0.35),rgba(255,255,255,0.2))]" />

      <main className="relative mx-auto flex max-w-6xl flex-col gap-6 px-6 pb-12 pt-10 sm:px-10">
        <header className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
          <div className="space-y-1">
            <p className="text-xs font-semibold uppercase tracking-[0.14em] text-amber-700/80">
              Тёплый дашборд
            </p>
            <h1 className="text-3xl font-semibold tracking-tight text-stone-950">
              My Analytics Dashboard
            </h1>
            <p className="text-sm text-stone-600">
              Тестовые данные из YouTube, Google Sheets и третьего источника.
            </p>
          </div>
          <div className="flex items-center gap-3">
            <div className="rounded-full border border-white/60 bg-white/60 px-4 py-2 text-xs font-medium text-stone-700 shadow-sm backdrop-blur">
              Обновлено: {formattedTime}
            </div>
            <button
              onClick={handleRefresh}
              className="flex items-center gap-2 rounded-full border border-white/60 bg-white/70 px-4 py-2 text-sm font-semibold text-amber-900 shadow-lg shadow-amber-100/70 backdrop-blur transition hover:-translate-y-0.5 hover:shadow-amber-200"
            >
              <span className="inline-flex h-2 w-2 rounded-full bg-amber-500 shadow-[0_0_0_6px_rgba(255,193,120,0.45)]" />
              Refresh data
            </button>
          </div>
        </header>

        <div className="grid gap-6 lg:grid-cols-3">
          <section className="glass-card lg:col-span-2">
            <div className="flex flex-wrap items-start justify-between gap-3">
              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.12em] text-amber-700/80">
                  YouTube
                </p>
                <h2 className="text-xl font-semibold text-stone-950">Channel health</h2>
                <p className="text-sm text-stone-600">Последние 7 дней · тестовые цифры</p>
              </div>
              <span className="rounded-full bg-amber-100 px-3 py-1 text-xs font-medium text-amber-900 shadow-inner shadow-white/70">
                Live preview
              </span>
            </div>

            <div className="mt-6 grid gap-4 sm:grid-cols-3">
              {youtubeStats.map((stat) => (
                <div
                  key={stat.label}
                  className="rounded-2xl border border-white/60 bg-white/60 p-4 shadow-inner shadow-white/60 backdrop-blur"
                >
                  <p className="text-sm text-stone-600">{stat.label}</p>
                  <div className="mt-2 flex items-baseline gap-2">
                    <span className="text-3xl font-semibold text-stone-950">
                      {formatNumber(stat.value)}
                      {stat.suffix && <span className="text-lg text-stone-600"> {stat.suffix}</span>}
                    </span>
                    <span className="rounded-full bg-amber-50 px-2 py-1 text-xs font-semibold text-amber-800">
                      {formatPercent(stat.delta)}
                    </span>
                  </div>
                </div>
              ))}
            </div>

            <div className="mt-6 grid gap-4 lg:grid-cols-2">
              <div className="rounded-2xl border border-white/60 bg-white/60 p-4 shadow-inner shadow-white/60 backdrop-blur">
                <div className="flex items-center justify-between">
                  <div>
                    <h3 className="text-base font-semibold text-stone-900">Top videos</h3>
                    <p className="text-sm text-stone-600">CTR и удержание · тестовые данные</p>
                  </div>
                  <span className="rounded-full bg-stone-900/80 px-3 py-1 text-xs font-semibold text-white">
                    7d
                  </span>
                </div>
                <div className="mt-4 space-y-3">
                  {youtubeVideos.map((video) => (
                    <div
                      key={video.title}
                      className="flex flex-wrap items-center justify-between gap-3 rounded-xl bg-white/70 px-3 py-2 shadow-sm shadow-white/70 backdrop-blur"
                    >
                      <div>
                        <p className="text-sm font-semibold text-stone-900">{video.title}</p>
                        <p className="text-xs text-stone-600">Views: {formatNumber(video.views)}</p>
                      </div>
                      <div className="flex items-center gap-2 text-xs font-semibold text-stone-800">
                        <span className="rounded-full bg-amber-100 px-2 py-1 text-amber-900">
                          CTR {video.ctr.toFixed(1)}%
                        </span>
                        <span className="rounded-full bg-white/80 px-2 py-1 text-stone-700">
                          Retention {video.retention}%
                        </span>
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              <div className="rounded-2xl border border-white/60 bg-white/60 p-4 shadow-inner shadow-white/60 backdrop-blur">
                <div className="flex items-center justify-between">
                  <div>
                    <h3 className="text-base font-semibold text-stone-900">Audience snapshot</h3>
                    <p className="text-sm text-stone-600">Профиль аудитории · пример</p>
                  </div>
                </div>
                <div className="mt-4 grid gap-3 sm:grid-cols-2">
                  <div className="rounded-xl bg-white/80 p-3 text-sm text-stone-800 shadow-sm">
                    <p className="text-xs uppercase tracking-[0.12em] text-amber-800">Top country</p>
                    <p className="mt-1 text-lg font-semibold text-stone-950">US · 38%</p>
                  </div>
                  <div className="rounded-xl bg-white/80 p-3 text-sm text-stone-800 shadow-sm">
                    <p className="text-xs uppercase tracking-[0.12em] text-amber-800">Top device</p>
                    <p className="mt-1 text-lg font-semibold text-stone-950">Mobile · 72%</p>
                  </div>
                  <div className="rounded-xl bg-white/80 p-3 text-sm text-stone-800 shadow-sm">
                    <p className="text-xs uppercase tracking-[0.12em] text-amber-800">Engaged viewers</p>
                    <p className="mt-1 text-lg font-semibold text-stone-950">12.6k</p>
                  </div>
                  <div className="rounded-xl bg-white/80 p-3 text-sm text-stone-800 shadow-sm">
                    <p className="text-xs uppercase tracking-[0.12em] text-amber-800">Traffic source</p>
                    <p className="mt-1 text-lg font-semibold text-stone-950">Search · 41%</p>
                  </div>
                </div>
              </div>
            </div>
          </section>

          <section className="glass-card">
            <div className="flex items-start justify-between gap-3">
              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.12em] text-amber-700/80">
                  Sales · Google Sheets
                </p>
                <h2 className="text-xl font-semibold text-stone-950">Pipeline snapshot</h2>
                <p className="text-sm text-stone-600">Тестовые строки, вместо выгрузки</p>
              </div>
              <div className="rounded-full bg-white/70 px-3 py-1 text-xs font-semibold text-stone-800 shadow-sm backdrop-blur">
                Total {formatNumber(totalRevenue)} $
              </div>
            </div>

            <div className="mt-5 space-y-3">
              {sales.map((row) => (
                <div
                  key={`${row.channel}-${row.region}`}
                  className="rounded-xl border border-white/60 bg-white/70 p-3 shadow-sm shadow-white/70 backdrop-blur"
                >
                  <div className="flex items-center justify-between text-sm">
                    <div>
                      <p className="text-base font-semibold text-stone-950">{row.channel}</p>
                      <p className="text-xs text-stone-600">{row.region}</p>
                    </div>
                    <span className="rounded-full bg-amber-100 px-3 py-1 text-xs font-semibold text-amber-900">
                      {row.orders} orders
                    </span>
                  </div>
                  <div className="mt-2 flex items-center justify-between text-sm text-stone-800">
                    <span className="font-semibold">${formatNumber(row.revenue)}</span>
                    <span className="text-xs text-stone-600">Avg ${formatNumber(row.avgOrder)}</span>
                  </div>
                </div>
              ))}
            </div>
          </section>

          <section className="glass-card lg:col-span-3">
            <div className="flex items-start justify-between gap-3">
              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.12em] text-amber-700/80">
                  Third Source
                </p>
                <h2 className="text-xl font-semibold text-stone-950">Заглушка с карточками</h2>
                <p className="text-sm text-stone-600">
                  Здесь пока тестовые карточки. Подменим настоящими данными позже.
                </p>
              </div>
              <span className="rounded-full bg-white/70 px-3 py-1 text-xs font-semibold text-stone-800 shadow-sm backdrop-blur">
                Placeholder
              </span>
            </div>

            <div className="mt-4 grid gap-4 md:grid-cols-3">
              {thirdCards.map((card) => (
                <div
                  key={card.title}
                  className="flex flex-col gap-3 rounded-xl border border-white/60 bg-white/70 p-4 shadow-sm shadow-white/80 backdrop-blur"
                >
                  <div className="flex items-center justify-between">
                    <p className="text-sm font-semibold text-stone-900">{card.title}</p>
                    <span className="rounded-full bg-amber-100 px-2 py-1 text-[11px] font-semibold text-amber-900">
                      {card.badge}
                    </span>
                  </div>
                  <p className="text-sm leading-relaxed text-stone-700">{card.body}</p>
                  <div className="flex items-center justify-between text-xs text-stone-600">
                    <span>Синхронизация: после интеграции</span>
                    <span className="rounded-full bg-white/80 px-2 py-1 text-[11px] font-semibold text-stone-700">
                      Mock
                    </span>
                  </div>
                </div>
              ))}
            </div>
          </section>
        </div>
      </main>
    </div>
  );
}
