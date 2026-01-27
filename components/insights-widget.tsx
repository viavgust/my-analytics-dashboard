"use client"

import { useEffect, useMemo, useRef, useState, type MouseEvent, type ReactNode } from "react"
import { RefreshCw, X } from "lucide-react"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Spinner } from "@/components/ui/spinner"
import { cn } from "@/lib/utils"

type InsightCard = {
  id: string
  source: "summary" | "ebay" | "telegram" | "youtube" | "calendar"
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

type HomeworkStatus = "yes" | "no" | null

const sourceStyles: Record<
  InsightCard["source"],
  { label: string; className: string }
> = {
  summary: { label: "Сводка", className: "bg-cyan-500/15 text-cyan-100 border border-cyan-200/20" },
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
  summary: 0,
  ebay: 1,
  telegram: 2,
  youtube: 3,
  calendar: 4,
}

function splitSummaryText(text: string) {
  const lines = text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
  let studyLine: string | null = null
  let homeworkLine: string | null = null
  if (lines[0]?.startsWith("Учёба:")) {
    studyLine = lines.shift() ?? null
  }
  if (lines[0]?.startsWith("Домашка:")) {
    homeworkLine = lines.shift() ?? null
  }
  return {
    studyLine,
    homeworkLine,
    body: lines.join("\n"),
  }
}

function getHomeworkKey(runDate: string | null) {
  const fallback = new Date().toISOString().slice(0, 10)
  return `homework_status_${runDate ?? fallback}`
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
  isSummary,
  homeworkStatus,
  onHomeworkChange,
}: {
  title: string
  badges: ReactNode[]
  text: string
  actions?: string[]
  isSummary?: boolean
  homeworkStatus?: HomeworkStatus
  onHomeworkChange?: (value: HomeworkStatus) => void
}) {
  const summaryParts = isSummary ? splitSummaryText(text) : null
  return (
    <div
      className={cn(
        "relative rounded-2xl border border-white/10 bg-white/5 px-4 py-4 shadow-[0_0_0_1px_rgba(255,255,255,0.04)] backdrop-blur",
        isSummary &&
          "border-amber-300/60 bg-amber-200/15 shadow-[0_0_0_1px_rgba(251,191,36,0.35)]"
      )}
    >
      {isSummary && (
        <span
          aria-hidden="true"
          className="absolute left-0 top-0 h-full w-1.5 rounded-l-2xl bg-amber-400/80"
        />
      )}
      <div className="flex items-center justify-between gap-2">
        <div className="min-w-0">
          <div
            className={cn(
              "flex items-center gap-2 font-semibold text-white",
              isSummary ? "text-base" : "text-sm"
            )}
          >
            {isSummary && (
              <span className="text-amber-300" aria-hidden="true">
                ⭐
              </span>
            )}
            <span>{title}</span>
          </div>
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

      {isSummary && summaryParts?.studyLine && (
        <div className="mt-3 text-sm text-amber-50/90">{summaryParts.studyLine}</div>
      )}
      {isSummary && (
        <div className="mt-1 flex flex-wrap items-center gap-2 text-sm text-amber-50/90">
          <span>Домашка: готова?</span>
          <button
            type="button"
            onClick={() => onHomeworkChange?.("yes")}
            aria-pressed={homeworkStatus === "yes"}
            className={cn(
              "rounded-full border px-2.5 py-1 text-xs font-semibold transition",
              homeworkStatus === "yes"
                ? "border-amber-200/90 bg-amber-300/40 text-amber-50 font-bold shadow-[0_0_0_1px_rgba(251,191,36,0.35)]"
                : "border-white/10 bg-white/5 text-amber-50/70 hover:border-amber-200/40"
            )}
          >
            Да
          </button>
          <button
            type="button"
            onClick={() => onHomeworkChange?.("no")}
            aria-pressed={homeworkStatus === "no"}
            className={cn(
              "rounded-full border px-2.5 py-1 text-xs font-semibold transition",
              homeworkStatus === "no"
                ? "border-amber-200/90 bg-amber-300/40 text-amber-50 font-bold shadow-[0_0_0_1px_rgba(251,191,36,0.35)]"
                : "border-white/10 bg-white/5 text-amber-50/70 hover:border-amber-200/40"
            )}
          >
            Нет
          </button>
        </div>
      )}
      {isSummary && summaryParts?.studyLine && (
        <div className="my-3 h-px w-full bg-white/10" />
      )}

      {renderInsightText(isSummary ? summaryParts?.body ?? "" : text)}

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
  const [homeworkStatus, setHomeworkStatus] = useState<HomeworkStatus>(null)
  const scrollLockRef = useRef<{ top: string; position: string; width: string; overflow: string } | null>(null)
  const scrollYRef = useRef(0)

  const baseUrl = useMemo(() => workerUrl?.replace(/\/$/, "") ?? "", [workerUrl])
  const homeworkKey = useMemo(() => getHomeworkKey(runDate), [runDate])

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

  useEffect(() => {
    if (!open) return
    try {
      const stored = localStorage.getItem(homeworkKey)
      if (stored === "yes" || stored === "no") {
        setHomeworkStatus(stored)
      } else {
        setHomeworkStatus(null)
      }
    } catch {
      setHomeworkStatus(null)
    }
  }, [open, homeworkKey])

  const handleHomeworkChange = (value: HomeworkStatus) => {
    setHomeworkStatus(value)
    try {
      if (value) {
        localStorage.setItem(homeworkKey, value)
      } else {
        localStorage.removeItem(homeworkKey)
      }
    } catch {
      // ignore storage errors
    }
  }

  const handleRefresh = (event: MouseEvent<HTMLButtonElement>) => {
    event.preventDefault()
    event.stopPropagation()
    void fetchInsights("generate")
  }

  const handleClose = (event?: MouseEvent<HTMLElement>) => {
    if (event) {
      event.preventDefault()
      event.stopPropagation()
    }
    setOpen(false)
  }

  const badgeContent = runDate ? `AI · ${runDate}` : "AI"

  useEffect(() => {
    if (!open) return
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setOpen(false)
      }
    }
    window.addEventListener("keydown", onKeyDown)
    return () => window.removeEventListener("keydown", onKeyDown)
  }, [open])

  useEffect(() => {
    if (typeof document === "undefined") return
    const body = document.body
    if (open) {
      scrollYRef.current = window.scrollY || window.pageYOffset || 0
      scrollLockRef.current = {
        top: body.style.top,
        position: body.style.position,
        width: body.style.width,
        overflow: body.style.overflow,
      }
      body.style.position = "fixed"
      body.style.top = `-${scrollYRef.current}px`
      body.style.width = "100%"
      body.style.overflow = "hidden"
      return () => {
        if (!scrollLockRef.current) return
        body.style.position = scrollLockRef.current.position
        body.style.top = scrollLockRef.current.top
        body.style.width = scrollLockRef.current.width
        body.style.overflow = scrollLockRef.current.overflow
        const y = scrollYRef.current
        scrollLockRef.current = null
        window.scrollTo(0, y)
      }
    }
    if (scrollLockRef.current) {
      body.style.position = scrollLockRef.current.position
      body.style.top = scrollLockRef.current.top
      body.style.width = scrollLockRef.current.width
      body.style.overflow = scrollLockRef.current.overflow
      const y = scrollYRef.current
      scrollLockRef.current = null
      window.scrollTo(0, y)
    }
  }, [open])

  return (
    <>
      <button
        type="button"
        onClick={() => setOpen((prev) => !prev)}
        className={cn(
          "group fixed right-4 top-[calc(env(safe-area-inset-top)+1rem)] z-[60] sm:top-4",
          open && "pointer-events-none"
        )}
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

      {open && (
        <>
          <div
            className="fixed inset-0 z-40 bg-black/50"
            onClick={handleClose}
            aria-hidden="true"
          />
          <aside
            className="fixed right-0 top-0 bottom-0 z-50 flex h-[100dvh] w-[92vw] flex-col overflow-hidden bg-[#1a1814] text-white border-l border-white/10 shadow-2xl shadow-black/40 pt-[env(safe-area-inset-top)] pb-[env(safe-area-inset-bottom)] sm:max-w-[420px]"
            role="dialog"
            aria-modal="false"
          >
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
                  onClick={handleRefresh}
                  disabled={loading}
                  title="Запустить генерацию"
                  aria-label="Запустить генерацию"
                >
                  {loading ? <Spinner className="h-4 w-4" /> : <RefreshCw className="h-4 w-4" />}
                </Button>
                <Button
                  variant="ghost"
                  size="icon-sm"
                  className="text-amber-200 hover:bg-amber-500/10 h-11 w-11 sm:h-9 sm:w-9"
                  onClick={handleClose}
                  title="Закрыть"
                  aria-label="Закрыть"
                >
                  <X className="h-4 w-4" />
                </Button>
              </div>
            </div>

            <ScrollArea className="flex-1 min-h-0 overscroll-contain touch-pan-y px-4 py-3">
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
                    const isSummary = card.source === "summary"
                    const metaBadges = isSummary
                      ? [
                          <span
                            key="summary"
                            className="rounded-full border border-amber-200/40 bg-amber-400/15 px-2 py-0.5 text-amber-200"
                          >
                            Главное
                          </span>,
                        ]
                      : [
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
                        badges={metaBadges}
                        text={card.text}
                        actions={card.actions}
                        isSummary={isSummary}
                        homeworkStatus={isSummary ? homeworkStatus : undefined}
                        onHomeworkChange={isSummary ? handleHomeworkChange : undefined}
                      />
                    )
                  })}
              </div>
            </ScrollArea>
          </aside>
        </>
      )}
    </>
  )
}
