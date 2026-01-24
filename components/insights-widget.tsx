"use client"

import { useEffect, useMemo, useState } from "react"
import { Bot, RefreshCw, X } from "lucide-react"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Drawer,
  DrawerContent,
} from "@/components/ui/drawer"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Spinner } from "@/components/ui/spinner"
import { cn } from "@/lib/utils"

type InsightCard = {
  id: string
  source: "ebay" | "telegram" | "youtube" | "calendar"
  type: "money" | "margin" | "action" | "signal" | "plan" | "recommendation"
  period: "7d" | "30d" | "90d" | "180d" | "today" | "week" | "3d"
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
  money: "Деньги",
  margin: "Маржа",
  action: "Действие",
  signal: "Рекомендация",
  recommendation: "Рекомендация",
  plan: "План",
}

const periodLabels: Record<InsightCard["period"], string> = {
  "7d": "7 дней",
  "30d": "30 дней",
  "90d": "90 дней",
  "180d": "180 дней",
  today: "Сегодня",
  week: "Неделя",
  "3d": "3 дня",
}

const SOURCE_ORDER: Record<InsightCard["source"], number> = {
  ebay: 0,
  telegram: 1,
  youtube: 2,
  calendar: 3,
}

function renderInsightText(text: string) {
  const lines = text
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter(Boolean)

  const bulletLike = lines.filter((l) => /^[-•]/.test(l))
  if (bulletLike.length >= 2) {
    return (
      <ul className="mt-2 list-inside list-disc space-y-1 text-sm leading-relaxed text-white/75">
        {bulletLike.map((l, i) => (
          <li key={i}>{l.replace(/^[-•]\s*/, "")}</li>
        ))}
      </ul>
    )
  }

  return (
    <div className="mt-2 whitespace-pre-line text-sm leading-relaxed text-white/75">
      {text}
    </div>
  )
}

function InsightCardView({
  title,
  badges,
  text,
  actions,
}: {
  title: string
  badges: string[]
  text: string
  actions?: string[]
}) {
  return (
    <div className="rounded-2xl border border-white/10 bg-white/5 px-4 py-4 shadow-[0_0_0_1px_rgba(255,255,255,0.04)] backdrop-blur">
      <div className="flex items-center justify-between gap-2">
        <div className="min-w-0">
          <div className="text-sm font-semibold text-white">{title}</div>
          {badges.length > 0 && (
            <div className="mt-1 flex flex-wrap gap-2 text-xs text-white/80">
              {badges.map((b, i) => (
                <span key={i} className="rounded-full px-2 py-0.5 font-semibold">
                  {b}
                </span>
              ))}
            </div>
          )}
        </div>
      </div>

      {renderInsightText(text)}

      {actions && actions.length > 0 && (
        <ul className="mt-3 space-y-1 text-sm text-amber-50/85">
          {actions.map((a, i) => (
            <li key={i} className="flex items-start gap-2">
              <span className="mt-1 h-1.5 w-1.5 rounded-full bg-amber-300" />
              <span>{a}</span>
            </li>
          ))}
        </ul>
      )}

      <div className="mt-3 max-h-56 overflow-auto pr-1" />
    </div>
  )
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
      const isJson = res.headers.get("content-type")?.includes("application/json")
      const json = (isJson ? await res.json() : null) as InsightsResponse | null

      if (!res.ok) {
        const detail = (json as any)?.message || (json as any)?.error || res.statusText
        throw new Error(`HTTP ${res.status}${detail ? `: ${detail}` : ""}`)
      }

      setInsights(json?.insights ?? [])
      setRunDate(json?.runDate ?? null)
    } catch (err: any) {
      console.error("Insights fetch failed", err)
      const message = err?.message ? ` (${err.message})` : ""
      setError(`Не удалось загрузить инсайты${message}`)
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
      <button
        type="button"
        onClick={() => setOpen((prev) => !prev)}
        className="group fixed bottom-4 right-4 z-[999] sm:top-4 sm:bottom-auto"
        aria-label="AI Insights"
      >
        <div className="relative">
          <img
            src="/robot-ai.svg"
            alt="AI robot"
            className="h-24 w-auto drop-shadow-[0_10px_20px_rgba(14,165,233,0.35)] transition-transform duration-200 group-hover:-translate-y-1 group-hover:scale-[1.04]"
          />
          <Badge className="absolute -top-2 -right-3 rounded-full bg-gray-900 px-1.5 py-0 text-[10px] font-semibold text-amber-200">
            AI
          </Badge>
        </div>
      </button>

      <DrawerContent className="data-[vaul-drawer-direction=right]:w-[92vw] data-[vaul-drawer-direction=right]:sm:max-w-[420px] bg-[#1a1814] text-white border-l border-white/10 shadow-2xl shadow-black/40">
        <div className="flex items-start justify-between gap-3 border-b border-white/10 p-4">
          <div>
            <p className="text-xs uppercase tracking-[0.14em] text-amber-200/70">AI Insights</p>
            <p className="text-sm text-amber-100/80">Инсайты и действия для дашборда</p>
            <p className="text-[11px] text-amber-100/60">
              {runDate ? `Обновлено: ${runDate.slice(0, 10)}` : "Последний прогон: —"}
            </p>
          </div>
          <div className="flex items-center gap-2">
            <Button
              variant="ghost"
              size="icon-sm"
              className="text-amber-200 hover:bg-amber-500/10 h-11 w-11 sm:h-9 sm:w-9"
              onClick={() => fetchInsights("generate")}
              disabled={loading}
              title="Запустить генерацию"
              aria-label="Запустить генерацию"
            >
              {loading ? <Spinner className="h-4 w-4" /> : <RefreshCw className="h-4 w-4" />}
            </Button>
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
            {insights
              .map((card, idx) => ({ ...card, _idx: idx }))
              .sort(
                (a, b) =>
                  (SOURCE_ORDER[a.source] ?? 999) - (SOURCE_ORDER[b.source] ?? 999) ||
                  a._idx - b._idx
              )
              .map((card) => {
              const sourceMeta = sourceStyles[card.source] ?? sourceStyles.ebay
              const metaBadges = [
                <span key="source" className={cn("rounded-full px-2 py-0.5", sourceMeta.className)}>
                  {sourceMeta.label}
                </span>,
                <span key="type" className="rounded-full bg-white/5 px-2 py-0.5 text-white/75">
                  {typeLabels[card.type]}
                </span>,
                <span key="period" className="rounded-full bg-white/5 px-2 py-0.5 text-white/75">
                  {periodLabels[card.period]}
                </span>,
              ]
              return (
                <InsightCardView
                  key={card.id}
                  title={card.title}
                  badges={metaBadges as any}
                  text={card.text}
                  actions={card.actions}
                />
              )
            })}
          </div>
        </ScrollArea>
      </DrawerContent>
    </Drawer>
  )
}
