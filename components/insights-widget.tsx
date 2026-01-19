"use client"

import { useEffect, useMemo, useState } from "react"
import { Bot, RefreshCw, Sparkles, X } from "lucide-react"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Drawer,
  DrawerClose,
  DrawerContent,
} from "@/components/ui/drawer"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Spinner } from "@/components/ui/spinner"
import { cn } from "@/lib/utils"

type InsightCard = {
  id: string
  source: "ebay" | "telegram" | "youtube" | "calendar"
  type: "money" | "margin" | "action" | "signal" | "plan"
  period: "7d" | "30d" | "90d" | "180d" | "today" | "week"
  title: string
  text: string
  actions?: string[]
}

type InsightsResponse = {
  runDate: string | null
  insights: InsightCard[]
}

const sourceStyles: Record<
  InsightCard["source"],
  { label: string; className: string }
> = {
  ebay: { label: "eBay", className: "bg-amber-500/15 text-amber-200 border border-amber-200/20" },
  telegram: { label: "Telegram", className: "bg-sky-500/15 text-sky-100 border border-sky-200/20" },
  youtube: { label: "YouTube", className: "bg-red-500/15 text-red-100 border border-red-200/20" },
  calendar: { label: "Calendar", className: "bg-emerald-500/15 text-emerald-100 border border-emerald-200/20" },
}

const typeLabels: Record<InsightCard["type"], string> = {
  money: "Money",
  margin: "Margin",
  action: "Action",
  signal: "Signal",
  plan: "Plan",
}

const periodLabels: Record<InsightCard["period"], string> = {
  "7d": "7d",
  "30d": "30d",
  "90d": "90d",
  "180d": "180d",
  today: "Today",
  week: "Week",
}

export function InsightsWidget({ workerUrl }: { workerUrl?: string }) {
  const [open, setOpen] = useState(false)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [insights, setInsights] = useState<InsightCard[]>([])
  const [runDate, setRunDate] = useState<string | null>(null)

  const baseUrl = useMemo(() => workerUrl?.replace(/\/$/, "") ?? "", [workerUrl])

  const fetchInsights = async (mode: "latest" | "generate" = "latest") => {
    setLoading(true)
    setError(null)
    if (!baseUrl) {
      setError("workerUrl не настроен")
      setLoading(false)
      return
    }
    try {
      const res = await fetch(`${baseUrl}/api/insights/${mode === "latest" ? "latest" : "generate"}`, {
        method: mode === "latest" ? "GET" : "POST",
      })
      const json = (await res.json()) as InsightsResponse
      setInsights(json.insights ?? [])
      setRunDate(json.runDate ?? null)
    } catch (err: any) {
      console.error("Insights fetch failed", err)
      setError("Не удалось загрузить инсайты. Попробуй обновить позже.")
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    if (open && insights.length === 0 && baseUrl) {
      void fetchInsights("latest")
    }
  }, [open, insights.length, baseUrl])

  const badgeContent = runDate ? `AI · ${runDate}` : "AI"

  return (
    <Drawer direction="right" open={open} onOpenChange={setOpen}>
      <Button
        variant="secondary"
        size="icon-lg"
        className="group fixed bottom-4 right-4 z-[60] h-14 w-14 rounded-full bg-amber-400 text-gray-900 shadow-lg shadow-amber-500/30 hover:bg-amber-300 sm:top-4 sm:bottom-auto"
        onClick={() => setOpen(true)}
        title="AI Insights"
      >
        <div className="relative flex items-center justify-center">
          <Bot className="h-5 w-5" />
          <Badge className="absolute -top-2 -right-2 rounded-full bg-gray-900 px-1.5 py-0 text-[10px] font-semibold text-amber-200">
            AI
          </Badge>
        </div>
      </Button>

      <DrawerContent className="data-[vaul-drawer-direction=right]:w-[92vw] data-[vaul-drawer-direction=right]:sm:max-w-[420px] bg-[#1a1814] text-white border-l border-white/10 shadow-2xl shadow-black/40">
        <div className="flex items-start justify-between gap-3 border-b border-white/10 p-4">
          <div>
            <p className="text-xs uppercase tracking-[0.14em] text-amber-200/70">AI Insights</p>
            <p className="text-sm text-amber-100/80">Инсайты и действия для дашборда</p>
            <p className="text-[11px] text-amber-100/60">
              {runDate ? `Обновлено: ${runDate}` : "Последний прогон: —"}
            </p>
          </div>
          <div className="flex items-center gap-2">
            <Button
              variant="ghost"
              size="icon-sm"
              className="text-amber-200 hover:bg-amber-500/10"
              onClick={() => fetchInsights("generate")}
              disabled={loading}
              title="Запустить генерацию"
            >
              {loading ? <Spinner className="h-4 w-4" /> : <RefreshCw className="h-4 w-4" />}
            </Button>
            <DrawerClose asChild>
              <Button
                variant="ghost"
                size="icon-sm"
                className="text-amber-200 hover:bg-amber-500/10"
                title="Закрыть"
              >
                <X className="h-4 w-4" />
              </Button>
            </DrawerClose>
          </div>
        </div>

        <ScrollArea className="h-[78vh] px-4 py-3">
          {loading && (
            <div className="flex items-center gap-2 rounded-xl border border-white/10 bg-white/5 p-3 text-sm text-amber-100/80">
              <Spinner className="h-4 w-4" />
              Загружаю инсайты...
            </div>
          )}

          {!loading && error && (
            <div className="rounded-xl border border-red-500/30 bg-red-500/10 p-3 text-sm text-red-100">
              {error}
            </div>
          )}

          {!loading && !error && insights.length === 0 && (
            <div className="rounded-xl border border-white/10 bg-white/5 p-3 text-sm text-amber-100/80">
              Нет инсайтов. Попробуй обновить.
            </div>
          )}

          <div className="mt-2 space-y-3">
            {insights.map((card) => {
              const sourceMeta = sourceStyles[card.source] ?? sourceStyles.ebay
              return (
                <div
                  key={card.id}
                  className="rounded-xl border border-white/10 bg-white/5 p-4 shadow-sm shadow-black/20"
                >
                  <div className="flex items-center justify-between gap-2 text-xs">
                    <div className="flex items-center gap-2">
                      <span className={cn("rounded-full px-2 py-0.5 font-semibold", sourceMeta.className)}>
                        {sourceMeta.label}
                      </span>
                      <span className="text-amber-100/80">{typeLabels[card.type]}</span>
                      <span className="text-amber-100/60">·</span>
                      <span className="text-amber-100/60">{periodLabels[card.period]}</span>
                    </div>
                  </div>
                  <p className="mt-2 text-sm font-semibold text-white">{card.title}</p>
                  <p className="mt-1 text-sm leading-relaxed text-amber-50/80">{card.text}</p>
                  {card.actions && card.actions.length > 0 && (
                    <ul className="mt-2 space-y-1 text-xs text-amber-50/80">
                      {card.actions.map((action, idx) => (
                        <li key={idx} className="flex items-start gap-2">
                          <span className="mt-1 h-1.5 w-1.5 rounded-full bg-amber-300" />
                          <span>{action}</span>
                        </li>
                      ))}
                    </ul>
                  )}
                </div>
              )
            })}
          </div>
        </ScrollArea>
      </DrawerContent>
    </Drawer>
  )
}
