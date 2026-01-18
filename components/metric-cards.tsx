import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { MessageCircle, Youtube, ShoppingBag } from "lucide-react"
import { type DashboardTelegram, type DashboardYouTube, type DashboardSales } from "@/lib/dashboard-types"

type MetricCardsProps = {
  telegram: DashboardTelegram
  youtube: DashboardYouTube
  sales: DashboardSales
}

function formatNumber(value: number, allowNA = false) {
  if (allowNA && (value === undefined || value === null || Number.isNaN(Number(value)))) return "N/A"
  return Number.isFinite(Number(value)) ? Number(value).toLocaleString() : "N/A"
}

function formatCurrency(value: number) {
  return `$${value.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 2 })}`
}

function formatRelativeTime(publishedAt: string) {
  const target = new Date(publishedAt).getTime()
  if (Number.isNaN(target)) return "—"
  const diffMs = Date.now() - target
  const diffMinutes = Math.max(0, Math.floor(diffMs / (1000 * 60)))
  const diffHours = Math.floor(diffMinutes / 60)
  const diffDays = Math.floor(diffHours / 24)

  if (diffMinutes < 60) return `${diffMinutes || 1}m ago`
  if (diffHours < 24) return `${diffHours}h ago`
  return `${diffDays}d ago`
}

function formatDate(dateString: string) {
  const date = new Date(dateString)
  if (Number.isNaN(date.getTime())) return dateString
  return date.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" })
}

export function MetricCards({ telegram, youtube, sales }: MetricCardsProps) {
  return (
    <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
      {/* Telegram Card */}
      <Card className="bg-gradient-to-br from-[#2a2520]/80 to-[#1e1a16]/80 backdrop-blur-xl border-white/10 rounded-2xl">
        <CardHeader className="pb-2">
          <div className="flex items-center gap-3">
            <div className="p-2 rounded-xl bg-blue-500/20">
              <MessageCircle className="w-5 h-5 text-blue-400" />
            </div>
            <div>
              <CardTitle className="text-white text-lg">Telegram</CardTitle>
              <p className="text-gray-500 text-sm">Latest 3 posts from channel</p>
            </div>
          </div>
        </CardHeader>
        <CardContent className="space-y-3">
          {(telegram.posts ?? []).map((post) => (
            <a
              key={post.messageId}
              href={post.url ?? "#"}
              target="_blank"
              rel="noopener noreferrer"
              className="block p-3 rounded-xl bg-white/5 border border-white/5 hover:bg-white/10 transition cursor-pointer"
            >
              <p className="text-gray-300 text-sm line-clamp-1">{post.text}</p>
              <p className="text-gray-500 text-xs mt-1">{formatRelativeTime(post.publishedAt)}</p>
            </a>
          ))}
        </CardContent>
      </Card>

      {/* YouTube Card */}
      <Card className="bg-gradient-to-br from-[#2a2520]/80 to-[#1e1a16]/80 backdrop-blur-xl border-white/10 rounded-2xl">
        <CardHeader className="pb-2">
          <div className="flex items-center gap-3">
            <div className="p-2 rounded-xl bg-red-500/20">
              <Youtube className="w-5 h-5 text-red-400" />
            </div>
            <div>
              <CardTitle className="text-white text-lg">YouTube</CardTitle>
              <p className="text-gray-500 text-sm">Latest 3 videos from channel</p>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <div className="space-y-2">
            {(youtube.latestVideos ?? []).slice(0, 3).map((video) => (
              <a
                key={video.videoId ?? video.url}
                href={video.url}
                target="_blank"
                rel="noopener noreferrer"
                className="p-3 rounded-xl bg-white/5 border border-white/5 flex gap-3 items-center hover:bg-white/10 transition cursor-pointer"
              >
                {video.thumbnailUrl ? (
                  <img
                    src={video.thumbnailUrl}
                    alt={video.title}
                    className="w-12 h-12 rounded-lg object-cover flex-shrink-0"
                  />
                ) : (
                  <div className="w-12 h-12 rounded-lg bg-white/5 flex-shrink-0" />
                )}
                <div className="min-w-0">
                  <p className="text-gray-300 text-sm line-clamp-1">{video.title}</p>
                  <p className="text-gray-500 text-xs mt-1">{formatRelativeTime(video.publishedAt)}</p>
                </div>
              </a>
            ))}
            {(youtube.latestVideos ?? []).length === 0 && (
              <div className="p-3 rounded-xl bg-white/5 border border-white/5">
                <p className="text-gray-500 text-sm">No videos yet</p>
              </div>
            )}
          </div>
        </CardContent>
      </Card>

      {/* eBay Sales Card */}
      <Card className="bg-gradient-to-br from-[#2a2520]/80 to-[#1e1a16]/80 backdrop-blur-xl border-white/10 rounded-2xl">
        <CardHeader className="pb-2">
          <div className="flex items-center gap-3">
            <div className="p-2 rounded-xl bg-green-500/20">
              <ShoppingBag className="w-5 h-5 text-green-400" />
            </div>
            <div>
              <CardTitle className="text-white text-lg">eBay Sales</CardTitle>
              <p className="text-gray-500 text-sm">Sales performance</p>
            </div>
          </div>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="grid grid-cols-2 gap-4">
            <MetricItem label="Total sales" value={formatNumber(sales.metrics.totalSales)} />
            <MetricItem label="Total revenue" value={formatCurrency(sales.metrics.totalRevenue)} highlight />
            <MetricItem label="Total profit" value={formatCurrency(sales.metrics.totalProfit)} />
            <MetricItem label="Avg. profit" value={formatCurrency(sales.metrics.avgProfit)} />
          </div>
          {sales.chart.points.length > 0 && (
            <div className="px-3 py-4 rounded-xl bg-white/5 border border-white/5 flex items-center justify-between">
              <span className="text-xs text-gray-500">Latest sale</span>
              <span className="text-xl font-bold text-white">
                {formatDate(sales.chart.points[sales.chart.points.length - 1].label)} • {formatCurrency(sales.chart.points[sales.chart.points.length - 1].revenue)}
              </span>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}

function MetricItem({ label, value, highlight = false }: { label: string; value: string; highlight?: boolean }) {
  return (
    <div className="p-3 rounded-xl bg-white/5 border border-white/5">
      <p className="text-gray-500 text-xs">{label}</p>
      <p className={`text-xl font-bold mt-1 ${highlight ? "text-green-400" : "text-white"}`}>{value}</p>
    </div>
  )
}
