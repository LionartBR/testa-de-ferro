// GrafoMini — compact preview of the corporate ownership graph.
//
// Design decisions:
// - A full D3/force-directed render here would be wasteful: this is a
//   detail in a scrollable dossier, not a primary visualization surface.
//   Instead, we show the node/edge counts + a prominent link to the full
//   graph page, which is the dedicated visualization surface.
// - truncado flag from the API means the graph was capped at 50 nodes.
//   We show a warning so the analyst knows to check the full page.
// - The link is rendered as a styled anchor (not Button) because it navigates
//   to a new route — semantically it is a link, not an action.

import { Link } from "react-router-dom";
import { Card, CardHeader } from "@/components/ui/Card";
import { EmptyState } from "@/components/ui/EmptyState";
import { cnpjToParam } from "@/lib/formatters";
import type { Grafo } from "@/types/grafo";

interface GrafoMiniProps {
  cnpj: string;
  grafo: Grafo | null;
}

export function GrafoMini({ cnpj, grafo }: GrafoMiniProps) {
  const graphUrl = `/fornecedores/${cnpjToParam(cnpj)}/grafo`;

  return (
    <Card>
      <CardHeader
        title="Grafo Societário"
        action={
          <Link
            to={graphUrl}
            className="text-xs font-medium text-blue-600 hover:underline"
          >
            Ver completo
          </Link>
        }
      />

      {!grafo || grafo.nos.length === 0 ? (
        <EmptyState message="Nenhuma relação societária identificada." />
      ) : (
        <div className="space-y-3">
          <div className="flex gap-6">
            <div className="text-center">
              <p className="text-2xl font-semibold tabular-nums text-gray-900">
                {grafo.nos.length}
              </p>
              <p className="text-xs text-gray-500">nós</p>
            </div>
            <div className="text-center">
              <p className="text-2xl font-semibold tabular-nums text-gray-900">
                {grafo.arestas.length}
              </p>
              <p className="text-xs text-gray-500">conexões</p>
            </div>
          </div>

          {grafo.truncado && (
            <p className="rounded-md border border-yellow-200 bg-yellow-50 px-3 py-2 text-xs text-yellow-700">
              Grafo truncado em 50 nós. Acesse a visualização completa para
              navegar todas as relações.
            </p>
          )}

          <Link
            to={graphUrl}
            className="flex w-full items-center justify-center rounded-md border border-blue-200 bg-blue-50 py-3 text-sm font-medium text-blue-700 hover:bg-blue-100"
          >
            Abrir visualização interativa
          </Link>
        </div>
      )}
    </Card>
  );
}
