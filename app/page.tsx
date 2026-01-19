export const dynamic = "force-dynamic"
export const fetchCache = "force-no-store"

import { HeroSection } from "@/components/hero-section"
import { MetricCards } from "@/components/metric-cards"
import { ChartsRow } from "@/components/charts-row"
import { StudyCalendarCard } from "@/components/study-calendar-card"
import { InsightsWidget } from "@/components/insights-widget"
import {
  type DashboardResponse,
  type DashboardTelegram,
  type DashboardYouTube,
  type DashboardSales,
  type DashboardCalendarEvent,
} from "@/lib/dashboard-types"

const WORKER_URL =
  process.env.NEXT_PUBLIC_WORKER_PUBLIC_URL ||
  process.env.WORKER_PUBLIC_URL ||
  "http://localhost:8788"

const FALLBACK_DASHBOARD: DashboardResponse = {
  updatedAt: "2025-03-15T09:15:00Z",
  telegram: {
    channel: "my_channel",
    posts: [
      { messageId: "451", text: "Новый ролик! Ссылка в био...", publishedAt: "2025-03-15T07:10:00Z" },
      { messageId: "448", text: "Еженедельный дайджест: лучшие моменты...", publishedAt: "2025-03-14T12:05:00Z" },
      { messageId: "443", text: "Закулисье вчерашней съемки...", publishedAt: "2025-03-12T18:45:00Z" },
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
    topVideo: {
      title: "Demo top video",
      views: 12000,
      publishedAt: "2025-01-01T00:00:00Z",
      url: "https://youtube.com",
      videoId: "demo",
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
  calendar: [],
}

async function getDashboardData(): Promise<DashboardResponse> {
  if (!WORKER_URL) return FALLBACK_DASHBOARD

  try {
    const res = await fetch(`${WORKER_URL}/api/dashboard`, { cache: "no-store" })
    if (!res.ok) throw new Error(`Failed to fetch dashboard: ${res.status}`)
    const data = (await res.json()) as DashboardResponse
    return data
  } catch (error) {
    console.error("Dashboard fetch failed, using fallback", error)
    return FALLBACK_DASHBOARD
  }
}

export default async function DashboardPage() {
  const dashboard = await getDashboardData()
  const telegram: DashboardTelegram = dashboard.telegram ?? FALLBACK_DASHBOARD.telegram
  const youtube: DashboardYouTube = dashboard.youtube ?? FALLBACK_DASHBOARD.youtube
  const sales: DashboardSales = dashboard.sales ?? FALLBACK_DASHBOARD.sales
  const calendar: DashboardCalendarEvent[] = dashboard.calendar ?? []

  return (
    <div className="min-h-screen bg-[#1a1814] text-white p-6 md:p-8">
      <div className="max-w-7xl mx-auto space-y-6">
        <HeroSection updatedAt={dashboard.updatedAt} workerUrl={WORKER_URL} />
        <MetricCards telegram={telegram} youtube={youtube} sales={sales} />
        <ChartsRow sales={sales.chart.points} />
        <StudyCalendarCard events={calendar} />
        <InsightsWidget workerUrl={WORKER_URL} />
      </div>
    </div>
  )
}
