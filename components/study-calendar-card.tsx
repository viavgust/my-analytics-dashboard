import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { CalendarDays, BookOpen, ChevronRight } from "lucide-react"

const weekDays = [
  { day: "Mon", date: 9, hasClass: true },
  { day: "Tue", date: 10, hasClass: false },
  { day: "Wed", date: 11, hasClass: true },
  { day: "Thu", date: 12, hasClass: false },
  { day: "Fri", date: 13, hasClass: true },
  { day: "Sat", date: 14, hasClass: false },
  { day: "Sun", date: 15, hasClass: false },
]

const events = [
  { title: "Lecture: Calculus", time: "10:00", status: "done" as const },
  { title: "Seminar: Programming", time: "14:00", status: "done" as const },
  { title: "Deadline: Term Paper", time: "23:59", status: "plan" as const },
  { title: "Practice: Databases", time: "16:00", status: "plan" as const },
]

export function StudyCalendarCard() {
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
            <p className="text-gray-400 text-xs mb-3 uppercase tracking-wider">December 2025</p>
            <div className="flex gap-2">
              {weekDays.map((item) => (
                <div key={item.day} className="flex flex-col items-center gap-1">
                  <span className="text-gray-500 text-xs">{item.day}</span>
                  <div
                    className={`w-9 h-9 rounded-full flex items-center justify-center text-sm font-medium transition-colors ${
                      item.hasClass
                        ? "bg-amber-500/30 text-amber-300 ring-2 ring-amber-500/50"
                        : "bg-white/5 text-gray-400"
                    } ${item.date === 9 ? "ring-2 ring-white/30" : ""}`}
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
            {events.map((event, index) => (
              <div
                key={index}
                className="flex items-center justify-between p-3 rounded-xl bg-white/5 border border-white/5"
              >
                <div className="flex items-center gap-3">
                  <CalendarDays className="w-4 h-4 text-gray-500" />
                  <div>
                    <p className="text-white text-sm">{event.title}</p>
                    <p className="text-gray-500 text-xs">{event.time}</p>
                  </div>
                </div>
                <span
                  className={`text-xs px-2 py-1 rounded-full ${
                    event.status === "done" ? "bg-green-500/20 text-green-400" : "bg-amber-500/20 text-amber-400"
                  }`}
                >
                  {event.status === "done" ? "Done" : "Plan"}
                </span>
              </div>
            ))}
          </div>
        </div>

        {/* Open Full Calendar Button */}
        <div className="mt-6 pt-4 border-t border-white/5">
          <Button variant="ghost" className="w-full text-amber-400 hover:text-amber-300 hover:bg-white/5">
            Open full calendar
            <ChevronRight className="w-4 h-4 ml-2" />
          </Button>
        </div>
      </CardContent>
    </Card>
  )
}
