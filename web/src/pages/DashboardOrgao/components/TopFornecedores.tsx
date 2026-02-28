import type { TopFornecedorOrgao } from "@/types/orgao";
import { Table } from "@/components/ui/Table";
import { Card, CardHeader } from "@/components/ui/Card";
import { CNPJFormatado } from "@/components/CNPJFormatado";
import { ScoreBadge } from "@/components/ScoreBadge";
import { ValorMonetario } from "@/components/ValorMonetario";
import { EmptyState } from "@/components/ui/EmptyState";
import { formatNumber } from "@/lib/formatters";
import type { FaixaRisco } from "@/types/score";

interface TopFornecedoresProps {
  fornecedores: TopFornecedorOrgao[];
}

// Derives a FaixaRisco label from a raw score value.
// Thresholds mirror the backend score_service categorization.
function scoreFaixa(score: number): FaixaRisco {
  if (score >= 70) return "Critico";
  if (score >= 40) return "Alto";
  if (score >= 20) return "Moderado";
  return "Baixo";
}

export function TopFornecedores({ fornecedores }: TopFornecedoresProps) {
  if (fornecedores.length === 0) {
    return (
      <Card>
        <CardHeader title="Top Fornecedores" />
        <EmptyState message="Nenhum fornecedor encontrado." />
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader
        title="Top Fornecedores"
        subtitle={`${fornecedores.length} fornecedores com maior volume contratado`}
      />
      <Table<TopFornecedorOrgao>
        keyExtractor={(item) => item.cnpj}
        data={fornecedores}
        columns={[
          {
            key: "cnpj",
            header: "CNPJ",
            render: (item) => <CNPJFormatado cnpj={item.cnpj} />,
          },
          {
            key: "razao_social",
            header: "Razão Social",
            render: (item) => (
              <span className="max-w-[240px] truncate block" title={item.razao_social}>
                {item.razao_social}
              </span>
            ),
          },
          {
            key: "score_risco",
            header: "Score",
            render: (item) => (
              <ScoreBadge
                valor={item.score_risco}
                faixa={scoreFaixa(item.score_risco)}
              />
            ),
          },
          {
            key: "valor_total",
            header: "Valor Total",
            render: (item) => <ValorMonetario valor={item.valor_total} />,
            className: "text-right",
          },
          {
            key: "qtd_contratos",
            header: "Contratos",
            render: (item) => (
              <span className="tabular-nums">
                {formatNumber(item.qtd_contratos)}
              </span>
            ),
            className: "text-right",
          },
        ]}
      />
    </Card>
  );
}
