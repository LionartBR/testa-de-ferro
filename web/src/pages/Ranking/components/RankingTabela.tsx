import { Table } from "@/components/ui/Table";
import { ScoreBadge } from "@/components/ScoreBadge";
import { CNPJFormatado } from "@/components/CNPJFormatado";
import { ValorMonetario } from "@/components/ValorMonetario";
import type { FornecedorResumo } from "@/types/fornecedor";

interface RankedItem {
  posicao: number;
  fornecedor: FornecedorResumo;
}

interface RankingTabelaProps {
  data: FornecedorResumo[];
  offset: number;
}

export function RankingTabela({ data, offset }: RankingTabelaProps) {
  const ranked: RankedItem[] = data.map((fornecedor, index) => ({
    posicao: offset + index + 1,
    fornecedor,
  }));

  return (
    <Table<RankedItem>
      data={ranked}
      keyExtractor={(item) => item.fornecedor.cnpj}
      columns={[
        {
          key: "posicao",
          header: "#",
          className: "w-12",
          render: (item) => (
            <span className="font-mono text-gray-400">{item.posicao}</span>
          ),
        },
        {
          key: "cnpj",
          header: "CNPJ",
          render: (item) => <CNPJFormatado cnpj={item.fornecedor.cnpj} />,
        },
        {
          key: "razao_social",
          header: "Razão Social",
          render: (item) => (
            <span className="max-w-xs truncate">{item.fornecedor.razao_social}</span>
          ),
        },
        {
          key: "score",
          header: "Score",
          render: (item) => (
            <ScoreBadge
              valor={item.fornecedor.score_risco}
              faixa={item.fornecedor.faixa_risco}
            />
          ),
        },
        {
          key: "alertas",
          header: "Alertas",
          className: "text-right",
          render: (item) =>
            item.fornecedor.qtd_alertas > 0 ? (
              <span className="font-mono font-medium text-red-600">
                {item.fornecedor.qtd_alertas}
              </span>
            ) : (
              <span className="font-mono text-gray-400">0</span>
            ),
        },
        {
          key: "valor_total",
          header: "Valor Total",
          className: "text-right",
          render: (item) => (
            <ValorMonetario
              valor={item.fornecedor.valor_total}
              className="text-gray-700"
            />
          ),
        },
      ]}
    />
  );
}
