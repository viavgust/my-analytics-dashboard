"use client"

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { type DashboardSalesChartPoint } from "@/lib/dashboard-types"
import { Area, AreaChart, ResponsiveContainer, XAxis, YAxis, Tooltip } from "recharts"

type ChartsRowProps = {
  sales: DashboardSalesChartPoint[]
}

export function ChartsRow({ sales }: ChartsRowProps) {
  const yDomain: [number | ((dataMin: number) => number), number | ((dataMax: number) => number)] = [
    (dataMin: number) => (Number.isFinite(dataMin) ? dataMin * 0.9 : 0),
    (dataMax: number) => (Number.isFinite(dataMax) ? dataMax * 1.1 : 1),
  ]

  return (
    <div className="grid grid-cols-1 gap-6 mb-4">
      <Card className="bg-gradient-to-br from-[#2a2520]/80 to-[#1e1a16]/80 backdrop-blur-xl border-white/10 rounded-2xl">
        <CardHeader className="pb-1 pt-2 px-4">
          <CardTitle className="text-white text-base">eBay revenue over time</CardTitle>
        </CardHeader>
        <CardContent className="px-4 pb-2">
          <div className="h-24">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={sales}>
                <defs>
                  <linearGradient id="ebayGradient" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor="#22c55e" stopOpacity={0.4} />
                    <stop offset="100%" stopColor="#22c55e" stopOpacity={0} />
                  </linearGradient>
                </defs>
                <XAxis dataKey="label" axisLine={false} tickLine={false} tick={{ fill: "#6b7280", fontSize: 11 }} />
                <YAxis hide domain={yDomain} />
                <Tooltip
                  contentStyle={{
                    backgroundColor: "#2a2520",
                    border: "1px solid rgba(255,255,255,0.1)",
                    borderRadius: "12px",
                    color: "#fff",
                  }}
                  formatter={(value) => [`$${value}`, "Revenue"]}
                />
                <Area type="monotone" dataKey="revenue" stroke="#22c55e" strokeWidth={2} fill="url(#ebayGradient)" />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
