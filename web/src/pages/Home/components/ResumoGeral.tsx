import type { Stats } from "@/types/stats";
import { Card } from "@/components/ui/Card";
import { formatNumber } from "@/lib/formatters";

interface ResumoGeralProps {
  stats: Stats;
}

interface StatCardProps {
  label: string;
  value: number;
}

function StatCard({ label, value }: StatCardProps) {
  return (
    <Card className="text-center">
      <p className="text-2xl font-bold text-gray-900">{formatNumber(value)}</p>
      <p className="mt-1 text-xs text-gray-500">{label}</p>
    </Card>
  );
}

export function ResumoGeral({ stats }: ResumoGeralProps) {
  return (
    <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
      <StatCard label="Fornecedores" value={stats.total_fornecedores} />
      <StatCard label="Contratos" value={stats.total_contratos} />
      <StatCard label="Alertas" value={stats.total_alertas} />
    </div>
  );
}
