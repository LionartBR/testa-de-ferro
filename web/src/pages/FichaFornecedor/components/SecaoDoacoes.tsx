// SecaoDoacoes — electoral donations linked to the supplier's partners.
//
// Design decisions:
// - When via_socio is true a badge signals that the link is indirect
//   (donation made by a partner, not the company itself). This distinction
//   is important: indirect connections are still evidence, but the reader
//   should know the degree of separation.
// - Donations are financial data — ValorMonetario ensures consistent
//   pt-BR currency formatting with tabular-nums for alignment.
// - ano_eleicao is a number; no formatting needed beyond rendering it.

import { Card, CardHeader } from "@/components/ui/Card";
import { EmptyState } from "@/components/ui/EmptyState";
import { Badge } from "@/components/ui/Badge";
import { ValorMonetario } from "@/components/ValorMonetario";
import type { Doacao } from "@/types/doacao";

interface DoacaoRowProps {
  doacao: Doacao;
}

function DoacaoRow({ doacao }: DoacaoRowProps) {
  return (
    <li className="flex items-start justify-between gap-4 py-3">
      <div className="min-w-0 flex-1">
        <div className="flex flex-wrap items-center gap-2">
          <span className="text-sm font-medium text-gray-900">
            {doacao.candidato_nome}
          </span>
          <span className="text-xs text-gray-500">
            {doacao.candidato_partido} — {doacao.candidato_cargo}
          </span>
          {doacao.via_socio && (
            <Badge className="bg-yellow-100 text-yellow-800">Via sócio</Badge>
          )}
        </div>
        <p className="mt-0.5 text-xs text-gray-500">
          Eleição {doacao.ano_eleicao}
        </p>
      </div>

      <ValorMonetario valor={doacao.valor} className="shrink-0 text-sm font-semibold text-gray-900" />
    </li>
  );
}

interface SecaoDocacoesProps {
  doacoes: Doacao[];
}

export function SecaoDoacoes({ doacoes }: SecaoDocacoesProps) {
  return (
    <Card>
      <CardHeader
        title="Doações Eleitorais"
        subtitle={
          doacoes.length > 0
            ? `${doacoes.length} doação${doacoes.length !== 1 ? "ões" : ""} identificada${doacoes.length !== 1 ? "s" : ""}`
            : undefined
        }
      />

      {doacoes.length === 0 ? (
        <EmptyState message="Nenhuma doação eleitoral vinculada." />
      ) : (
        <ul className="divide-y divide-gray-100">
          {doacoes.map((doacao, index) => (
            <DoacaoRow key={index} doacao={doacao} />
          ))}
        </ul>
      )}
    </Card>
  );
}
