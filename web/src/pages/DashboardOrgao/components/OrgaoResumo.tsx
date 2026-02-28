import type { DashboardOrgao } from "@/types/orgao";
import { Card } from "@/components/ui/Card";
import { ValorMonetario } from "@/components/ValorMonetario";
import { formatNumber } from "@/lib/formatters";

interface OrgaoResumoProps {
  dashboard: DashboardOrgao;
}

interface StatCardProps {
  label: string;
  value: React.ReactNode;
}

function StatCard({ label, value }: StatCardProps) {
  return (
    <div className="rounded-lg border border-gray-100 bg-gray-50 px-4 py-3">
      <p className="text-xs font-medium uppercase tracking-wide text-gray-400">
        {label}
      </p>
      <p className="mt-1 text-lg font-bold text-gray-900">{value}</p>
    </div>
  );
}

export function OrgaoResumo({ dashboard }: OrgaoResumoProps) {
  const { orgao, qtd_contratos, total_contratado, qtd_fornecedores } =
    dashboard;

  const nomeCompleto = orgao.sigla
    ? `${orgao.sigla} — ${orgao.nome}`
    : orgao.nome;

  return (
    <Card>
      <div className="mb-4">
        <p className="font-mono text-xs text-gray-400">
          Código {orgao.codigo}
        </p>
        <h2 className="mt-1 text-lg font-bold text-gray-900">{nomeCompleto}</h2>
      </div>

      <div className="grid grid-cols-3 gap-3">
        <StatCard
          label="Total contratado"
          value={<ValorMonetario valor={total_contratado} />}
        />
        <StatCard
          label="Contratos"
          value={formatNumber(qtd_contratos)}
        />
        <StatCard
          label="Fornecedores"
          value={formatNumber(qtd_fornecedores)}
        />
      </div>
    </Card>
  );
}
