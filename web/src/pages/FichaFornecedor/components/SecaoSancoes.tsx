// SecaoSancoes — list of sanctions, vigentes highlighted with a red border.
//
// Design decisions:
// - Active (vigente) sanctions get a red left border to immediately draw
//   attention — they are the most actionable data point on the page.
// - Expired sanctions are shown without the left border and with lighter
//   text to signal that the risk has diminished.
// - data_fim null means the sanction has no end date (indefinite) — shown
//   as "Indefinida" rather than a blank, which would look like missing data.

import { Card, CardHeader } from "@/components/ui/Card";
import { EmptyState } from "@/components/ui/EmptyState";
import { Badge } from "@/components/ui/Badge";
import { formatDate } from "@/lib/formatters";
import type { Sancao } from "@/types/sancao";

interface SancaoRowProps {
  sancao: Sancao;
}

function SancaoRow({ sancao }: SancaoRowProps) {
  const borderClass = sancao.vigente
    ? "border-l-4 border-l-red-400 pl-3"
    : "pl-4";

  return (
    <li className={`py-3 ${borderClass}`}>
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2">
            <span className="font-mono text-xs font-semibold text-gray-700">
              {sancao.tipo}
            </span>
            {sancao.vigente ? (
              <Badge className="bg-red-100 text-red-800">Vigente</Badge>
            ) : (
              <Badge className="bg-gray-100 text-gray-600">Expirada</Badge>
            )}
          </div>
          <p className="mt-0.5 text-sm text-gray-800">{sancao.motivo}</p>
          <p className="mt-0.5 text-xs text-gray-500">{sancao.orgao_sancionador}</p>
        </div>

        <div className="shrink-0 text-right text-xs text-gray-500">
          <div>Início: {formatDate(sancao.data_inicio)}</div>
          <div>
            Fim:{" "}
            {sancao.data_fim ? formatDate(sancao.data_fim) : "Indefinida"}
          </div>
        </div>
      </div>
    </li>
  );
}

interface SecaoSancoesProps {
  sancoes: Sancao[];
}

export function SecaoSancoes({ sancoes }: SecaoSancoesProps) {
  const vigentes = sancoes.filter((s) => s.vigente);
  const subtitle =
    vigentes.length > 0
      ? `${vigentes.length} vigente${vigentes.length !== 1 ? "s" : ""} de ${sancoes.length} total`
      : `${sancoes.length} sanção${sancoes.length !== 1 ? "ões" : ""} histórica${sancoes.length !== 1 ? "s" : ""}`;

  return (
    <Card>
      <CardHeader title="Sanções" subtitle={sancoes.length > 0 ? subtitle : undefined} />

      {sancoes.length === 0 ? (
        <EmptyState message="Nenhuma sanção registrada." />
      ) : (
        <ul className="divide-y divide-gray-100">
          {sancoes.map((sancao, index) => (
            <SancaoRow key={index} sancao={sancao} />
          ))}
        </ul>
      )}
    </Card>
  );
}
