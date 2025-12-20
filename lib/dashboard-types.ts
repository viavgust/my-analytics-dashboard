export type DashboardTelegramPost = {
  messageId: string
  text: string
  publishedAt: string
  url?: string | null
}

export type DashboardTelegram = {
  channel?: string
  posts: DashboardTelegramPost[]
}

export type DashboardYouTubeMetrics = {
  viewsToday: number
  views7d: number
  views30d: number
  allTimeViews: number
  newVideos30d: number
  subscribers?: number
  videoCount?: number
}

export type DashboardTopVideo = {
  title: string
  views: number
  publishedAt?: string
  url?: string
  videoId?: string
}

export type DashboardYouTubeChartPoint = {
  label: string
  views: number
}

export type DashboardYouTubeVideo = {
  title: string
  url: string
  publishedAt: string
  thumbnailUrl?: string | null
  videoId?: string | null
}

export type DashboardYouTube = {
  metrics: DashboardYouTubeMetrics
  chart: {
    granularity: "day" | "month"
    points: DashboardYouTubeChartPoint[]
  }
  topVideo?: DashboardTopVideo | null
  latestVideos?: DashboardYouTubeVideo[]
}

export type DashboardSalesMetrics = {
  totalSales: number
  totalRevenue: number
  totalProfit: number
  avgProfit: number
}

export type DashboardSalesChartPoint = {
  label: string
  revenue: number
}

export type DashboardSales = {
  metrics: DashboardSalesMetrics
  chart: {
    granularity: "day" | "month"
    points: DashboardSalesChartPoint[]
  }
}

export type DashboardResponse = {
  updatedAt: string
  telegram: DashboardTelegram
  youtube: DashboardYouTube
  sales: DashboardSales
}
