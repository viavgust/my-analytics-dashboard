// @ts-nocheck
'use client'
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { CalendarDays, BookOpen, ChevronRight } from "lucide-react"
import type { DashboardCalendarEvent } from "@/lib/dashboard-types"

function formatLocalTime(iso: string) {
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return "—"
  return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })
}

function formatLocalDate(iso: string) {
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return ""
  return d.toLocaleDateString(undefined, { month: "short", day: "numeric" })
}

function buildWeekDays(events: DashboardCalendarEvent[]) {
  const today = new Date()
  const days = []
  for (let i = 0; i < 7; i++) {
    const d = new Date(today)
    d.setDate(today.getDate() + i)
    const label = d.toLocaleDateString(undefined, { weekday: "short" })
    const dateNum = d.getDate()
    const hasClass = events.some((ev) => {
      const start = new Date(ev.start)
      return (
        start.getFullYear() === d.getFullYear() &&
        start.getMonth() === d.getMonth() &&
        start.getDate() === d.getDate()
      )
    })
    days.push({ day: label, date: dateNum, hasClass, isToday: i === 0 })
  }
  return days
}

type Props = {
  events?: DashboardCalendarEvent[]
}

export function StudyCalendarCard({ events = [] }: Props) {
  const weekDays = buildWeekDays(events)
  const upcoming = events.slice(0, 5)
  const firstLink = upcoming.find((ev) => ev.url)?.url ?? "https://calendar.google.com"

  return (
    <Card className="bg-gradient-to-br from-[#2a2520]/80 to-[#1e1a16]/80 backdrop-blur-xl border-white/10 rounded-2xl">
      <CardHeader>
        <div className="flex items-center gap-3">
          <div className="p-2 rounded-xl bg-amber-500/20">
            <BookOpen className="w-5 h-5 text-amber-400" />
          </div>
          <div>
            <CardTitle className="text-white text-lg">Study & Schedule</CardTitle>
            <p className="text-gray-500 text-sm">Classes, deadlines and homework</p>
          </div>
        </div>
      </CardHeader>
      <CardContent>
        <div className="flex flex-col md:flex-row gap-6">
          {/* Mini Calendar */}
          <div className="flex-shrink-0">
            <p className="text-gray-400 text-xs mb-3 uppercase tracking-wider">
              {new Date().toLocaleDateString(undefined, { month: "long", year: "numeric" })}
            </p>
            <div className="flex gap-2">
              {weekDays.map((item) => (
                <div key={item.day} className="flex flex-col items-center gap-1">
                  <span className="text-gray-500 text-xs">{item.day}</span>
                  <div
                    className={`w-9 h-9 rounded-full flex items-center justify-center text-sm font-medium transition-colors ${
                      item.hasClass
                        ? "bg-amber-500/30 text-amber-300 ring-2 ring-amber-500/50"
                        : "bg-white/5 text-gray-400"
                    } ${item.isToday ? "ring-2 ring-white/30" : ""}`}
                  >
                    {item.date}
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Events List */}
          <div className="flex-1 space-y-2">
            <p className="text-gray-400 text-xs mb-3 uppercase tracking-wider">Upcoming events</p>
            {upcoming.map((event, index) => (
              <div key={`${event.title}-${index}`} className="flex items-center justify-between p-3 rounded-xl bg-white/5 border border-white/5">
                <div className="flex items-center gap-3">
                  <CalendarDays className="w-4 h-4 text-gray-500" />
                  <div>
                    <p className="text-white text-sm line-clamp-1">{event.title}</p>
                    <p className="text-gray-500 text-xs">
                      {formatLocalDate(event.start)} · {formatLocalTime(event.start)}
                      {event.end ? ` – ${formatLocalTime(event.end)}` : ""}
                    </p>
                  </div>
                </div>
                <span className="text-xs px-2 py-1 rounded-full bg-amber-500/20 text-amber-400">Upcoming</span>
              </div>
            ))}
            {upcoming.length === 0 && (
              <div className="flex items-center justify-between p-3 rounded-xl bg-white/5 border border-white/5">
                <div className="flex items-center gap-3">
                  <CalendarDays className="w-4 h-4 text-gray-500" />
                  <div>
                    <p className="text-white text-sm">No upcoming events</p>
                    <p className="text-gray-500 text-xs">Refresh to sync calendar</p>
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>

        {/* Open Full Calendar Button */}
        <div className="mt-6 pt-4 border-t border-white/5">
          <Button
            variant="ghost"
            className="w-full text-amber-400 hover:text-amber-300 hover:bg-white/5"
            onClick={() => window.open(firstLink, "_blank")}
          >
            Open full calendar
            <ChevronRight className="w-4 h-4 ml-2" />
          </Button>
        </div>
      </CardContent>
    </Card>
  )
}
