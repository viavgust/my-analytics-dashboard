import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { type DashboardTopVideo } from "@/lib/dashboard-types"
import Link from "next/link"

type TopVideoCardProps = {
  topVideo?: DashboardTopVideo | null
}

function formatNumber(value: number) {
  return value.toLocaleString()
}

function formatDate(value?: string) {
  if (!value) return "—"
  const d = new Date(value)
  if (Number.isNaN(d.getTime())) return "—"
  return d.getFullYear().toString()
}

export function TopVideoCard({ topVideo }: TopVideoCardProps) {
  const barWidth = topVideo ? (topVideo.views > 0 ? "100%" : "10%") : "0%";

  return (
    <Card className="bg-gradient-to-br from-[#2a2520]/80 to-[#1e1a16]/80 backdrop-blur-xl border-white/10 rounded-2xl">
      <CardHeader className="pb-2">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="p-2 rounded-xl bg-red-500/20">
              <span className="text-red-400 text-sm font-semibold">YT</span>
            </div>
            <div>
              <CardTitle className="text-white text-lg">Top video (all-time)</CardTitle>
              <p className="text-gray-500 text-sm">Most viewed video</p>
            </div>
          </div>
          {topVideo?.url ? (
            <Link
              href={topVideo.url}
              target="_blank"
              className="text-amber-400 text-xs hover:text-amber-300 transition"
            >
              Open
            </Link>
          ) : null}
        </div>
      </CardHeader>
      <CardContent className="space-y-3">
        {topVideo ? (
          <>
            <div>
              <p className="text-white text-sm font-semibold line-clamp-2">{topVideo.title}</p>
              <p className="text-gray-500 text-xs">
                {formatNumber(topVideo.views)} views · {formatDate(topVideo.publishedAt)}
              </p>
            </div>
            <div className="mt-2 h-3 w-full rounded-full bg-white/5 border border-white/10">
              <div
                className="h-full rounded-full bg-amber-400"
                style={{ width: barWidth }}
                aria-label="Top video views"
              />
            </div>
          </>
        ) : (
          <p className="text-gray-500 text-sm">No video data yet.</p>
        )}
      </CardContent>
    </Card>
  )
}
