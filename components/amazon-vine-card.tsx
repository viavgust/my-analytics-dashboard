import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Package, Truck, Star, Clock } from "lucide-react"

export function AmazonVineCard() {
  return (
    <Card className="bg-gradient-to-br from-[#2a2520]/80 to-[#1e1a16]/80 backdrop-blur-xl border-white/10 rounded-2xl">
      <CardHeader>
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="p-2 rounded-xl bg-amber-500/20">
              <Package className="w-5 h-5 text-amber-400" />
            </div>
            <div>
              <CardTitle className="text-white text-lg">Amazon Vine (Ultra)</CardTitle>
              <p className="text-gray-500 text-sm">Vine membership stats</p>
            </div>
          </div>
          <span className="text-xs text-gray-500 bg-white/5 px-3 py-1 rounded-full border border-white/10">
            Powered by Gmail parsing (Ultra)
          </span>
        </div>
      </CardHeader>
      <CardContent>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <VineStat icon={Package} label="Vine orders" value="24" color="text-amber-400" />
          <VineStat icon={Truck} label="Vine delivered" value="21" color="text-green-400" />
          <VineStat icon={Star} label="Vine reviews" value="18" color="text-blue-400" />
          <VineStat icon={Clock} label="Vine pending" value="3" color="text-orange-400" />
        </div>
      </CardContent>
    </Card>
  )
}

function VineStat({ icon: Icon, label, value, color }: { icon: any; label: string; value: string; color: string }) {
  return (
    <div className="p-4 rounded-xl bg-white/5 border border-white/5 text-center">
      <Icon className={`w-6 h-6 ${color} mx-auto mb-2`} />
      <p className="text-2xl font-bold text-white">{value}</p>
      <p className="text-gray-500 text-sm mt-1">{label}</p>
    </div>
  )
}
