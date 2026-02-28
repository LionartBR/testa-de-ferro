// PLACEHOLDER — chart implementation requires a charting library (e.g., Nivo).
// Displays a basic horizontal bar representation using Tailwind widths
// so the layout is meaningful without external dependencies.

import type { TopFornecedorOrgao } from "@/types/orgao";
import { Card, CardHeader } from "@/components/ui/Card";
import { ValorMonetario } from "@/components/ValorMonetario";
import { EmptyState } from "@/components/ui/EmptyState";

interface ContratosChartProps {
  fornecedores: TopFornecedorOrgao[];
}

function parseValor(value: string): number {
  const num = parseFloat(value);
  return isNaN(num) ? 0 : num;
}

export function ContratosChart({ fornecedores }: ContratosChartProps) {
  if (fornecedores.length === 0) {
    return (
      <Card>
        <CardHeader title="Distribuição por Fornecedor" />
        <EmptyState message="Sem dados para exibir." />
      </Card>
    );
  }

  const valores = fornecedores.map((item) => parseValor(item.valor_total));
  const maxValor = Math.max(...valores);

  return (
    <Card>
      <CardHeader
        title="Distribuição por Fornecedor"
        subtitle="Valor total contratado — visualização simplificada"
      />
      <div className="space-y-2">
        {fornecedores.map((item, index) => {
          const valor = valores[index] ?? 0;
          const widthPercent =
            maxValor > 0 ? Math.round((valor / maxValor) * 100) : 0;

          return (
            <div key={item.cnpj} className="flex items-center gap-3">
              <span
                className="w-40 shrink-0 truncate text-right text-xs text-gray-600"
                title={item.razao_social}
              >
                {item.razao_social}
              </span>
              <div className="flex-1 overflow-hidden rounded-sm bg-gray-100">
                <div
                  className="h-5 rounded-sm bg-blue-500 transition-all"
                  style={{ width: `${widthPercent}%` }}
                />
              </div>
              <span className="w-28 shrink-0 text-right text-xs tabular-nums text-gray-700">
                <ValorMonetario valor={item.valor_total} />
              </span>
            </div>
          );
        })}
      </div>
    </Card>
  );
}
