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
            <div key={post.messageId} className="p-3 rounded-xl bg-white/5 border border-white/5">
              <p className="text-gray-300 text-sm line-clamp-1">{post.text}</p>
              <p className="text-gray-500 text-xs mt-1">{formatRelativeTime(post.publishedAt)}</p>
            </div>
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
              <p className="text-gray-500 text-sm">Channel performance</p>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 gap-4">
            <MetricItem label="Subscribers" value={formatNumber(youtube.metrics.subscribers ?? 0, true)} />
            <MetricItem
              label="Videos"
              value={
                typeof youtube.metrics.videoCount === "number"
                  ? formatNumber(youtube.metrics.videoCount, true)
                  : "N/A"
              }
            />
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
        <CardContent>
          <div className="grid grid-cols-2 gap-4">
            <MetricItem label="Total sales" value={formatNumber(sales.metrics.totalSales)} />
            <MetricItem label="Total revenue" value={formatCurrency(sales.metrics.totalRevenue)} highlight />
            <MetricItem label="Total profit" value={formatCurrency(sales.metrics.totalProfit)} />
            <MetricItem label="Avg. profit" value={formatCurrency(sales.metrics.avgProfit)} />
          </div>
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
