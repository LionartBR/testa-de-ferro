// SecaoContratos — paginated table of contracts for a supplier.
//
// Design decisions:
// - Uses the generic Table<T> component with explicit column definitions.
// - Client-side pagination only: the contracts array is already fully loaded
//   as part of FichaCompleta. A separate API call per page is unnecessary
//   given the bounded size of a supplier's contract history.
// - objeto (contract description) is truncated to 60 chars to keep rows
//   scannable. Full text is shown in a title attribute for accessibility.
// - The header shows the total contract count and aggregate value so the
//   reader gets the financial exposure at a glance without scrolling.

import { useState } from "react";
import { Card, CardHeader } from "@/components/ui/Card";
import { EmptyState } from "@/components/ui/EmptyState";
import { Table } from "@/components/ui/Table";
import { ValorMonetario } from "@/components/ValorMonetario";
import { formatDate } from "@/lib/formatters";
import type { ContratoResumo } from "@/types/contrato";

const PAGE_SIZE = 10;

function truncate(text: string, max: number): string {
  return text.length > max ? text.slice(0, max) + "…" : text;
}

interface SecaoContratosProps {
  contratos: ContratoResumo[];
  totalContratos: number;
  valorTotalContratos: string;
}

export function SecaoContratos({
  contratos,
  totalContratos,
  valorTotalContratos,
}: SecaoContratosProps) {
  const [page, setPage] = useState(1);

  const totalPages = Math.ceil(contratos.length / PAGE_SIZE);
  const start = (page - 1) * PAGE_SIZE;
  const paginated = contratos.slice(start, start + PAGE_SIZE);

  const subtitle = `${totalContratos} contrato${totalContratos !== 1 ? "s" : ""} — total `;

  const columns = [
    {
      key: "orgao",
      header: "Órgão",
      render: (c: ContratoResumo) => (
        <span className="font-mono text-xs">{c.orgao_codigo}</span>
      ),
    },
    {
      key: "objeto",
      header: "Objeto",
      render: (c: ContratoResumo) =>
        c.objeto ? (
          <span title={c.objeto}>{truncate(c.objeto, 60)}</span>
        ) : (
          <span className="text-gray-400">—</span>
        ),
      className: "max-w-xs",
    },
    {
      key: "valor",
      header: "Valor",
      render: (c: ContratoResumo) => (
        <ValorMonetario valor={c.valor} className="text-right" />
      ),
      className: "text-right",
    },
    {
      key: "data",
      header: "Assinatura",
      render: (c: ContratoResumo) => formatDate(c.data_assinatura),
    },
  ];

  return (
    <Card>
      <CardHeader
        title="Contratos"
        subtitle={
          subtitle + new Intl.NumberFormat("pt-BR", { style: "currency", currency: "BRL" }).format(parseFloat(valorTotalContratos) || 0)
        }
      />

      {contratos.length === 0 ? (
        <EmptyState message="Nenhum contrato registrado." />
      ) : (
        <>
          <Table<ContratoResumo>
            columns={columns}
            data={paginated}
            keyExtractor={(c) => `${c.orgao_codigo}-${c.data_assinatura ?? ""}-${c.valor}`}
          />

          {totalPages > 1 && (
            <div className="mt-3 flex items-center justify-between text-xs text-gray-500">
              <span>
                Página {page} de {totalPages}
              </span>
              <div className="flex gap-2">
                <button
                  onClick={() => setPage((p) => Math.max(1, p - 1))}
                  disabled={page === 1}
                  className="rounded px-2 py-1 hover:bg-gray-100 disabled:opacity-40"
                >
                  Anterior
                </button>
                <button
                  onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
                  disabled={page === totalPages}
                  className="rounded px-2 py-1 hover:bg-gray-100 disabled:opacity-40"
                >
                  Próxima
                </button>
              </div>
            </div>
          )}
        </>
      )}
    </Card>
  );
}
