/* eslint-disable no-console */
"use client"

import { Button } from "@/components/ui/button"
import { RefreshCw } from "lucide-react"
import { useRouter } from "next/navigation"
import { useState } from "react"

type HeroSectionProps = {
  updatedAt?: string
  workerUrl?: string
}

function formatUpdatedAt(updatedAt?: string) {
  if (!updatedAt) return "Last update: —"
  const date = new Date(updatedAt)
  if (Number.isNaN(date.getTime())) return "Last update: —"
  const formatted = date.toLocaleString(undefined, {
    day: "numeric",
    month: "short",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  })
  return `Last update: ${formatted}`
}

export function HeroSection({ updatedAt, workerUrl }: HeroSectionProps) {
  const router = useRouter()
  const [isRefreshing, setIsRefreshing] = useState(false)

  const refreshEndpoint =
    workerUrl ?? process.env.NEXT_PUBLIC_WORKER_PUBLIC_URL ?? process.env.WORKER_PUBLIC_URL ?? "http://localhost:8787"

  const handleRefresh = async () => {
    setIsRefreshing(true)
    try {
      await fetch(`${refreshEndpoint}/api/refresh`, { method: "POST" })
      router.refresh()
    } catch (error) {
      console.error("Refresh failed", error)
    } finally {
      setIsRefreshing(false)
    }
  }

  return (
    <div className="relative overflow-hidden rounded-3xl bg-gradient-to-br from-[#2a2520]/80 to-[#1e1a16]/80 backdrop-blur-xl border border-white/10">
      <div className="absolute inset-0 bg-gradient-to-r from-orange-500/10 via-transparent to-amber-500/5" />
      <div className="relative flex flex-col items-center text-center gap-3 p-4 md:p-6">
        <div className="space-y-2 z-10">
          <p className="text-amber-200/60 text-xs font-medium">Personal Analytics</p>
          <h1 className="text-3xl md:text-4xl font-bold text-white leading-tight">My Analytics Dashboard</h1>
        </div>
        <Button
          onClick={handleRefresh}
          disabled={isRefreshing}
          className="bg-amber-400 hover:bg-amber-300 text-gray-900 rounded-full px-5 py-2 font-medium text-sm"
        >
          <RefreshCw className="w-3.5 h-3.5 mr-1.5" />
          {isRefreshing ? "Refreshing..." : "Refresh data"}
        </Button>
        <p className="text-gray-500 text-xs">{formatUpdatedAt(updatedAt)}</p>
      </div>
    </div>
  )
}
