// ContratosTimeline — vertical timeline of contracts sorted by date descending.
//
// Design decision: Tailwind-only, no chart library. The vertical spine with
// date connectors gives clear temporal context without the overhead of a full
// charting dependency. Contracts without a date are pushed to the end of the
// list and shown as "Data não informada" to avoid silent data gaps.
//
// Object text is truncated at 100 chars to keep row height consistent and
// avoid the layout collapsing with very long contract descriptions.

import type { ContratoResumo } from "@/types/contrato";
import { formatCurrency, formatDate } from "@/lib/formatters";

interface ContratosTimelineProps {
  contratos: ContratoResumo[];
}

const MAX_OBJETO_LENGTH = 100;

function truncate(text: string, maxLength: number): string {
  if (text.length <= maxLength) return text;
  return text.slice(0, maxLength).trimEnd() + "…";
}

// Sorts contracts with a valid date before those without, then by date desc.
function sortByDateDesc(contratos: ContratoResumo[]): ContratoResumo[] {
  return [...contratos].sort((a, b) => {
    if (a.data_assinatura === null && b.data_assinatura === null) return 0;
    if (a.data_assinatura === null) return 1;
    if (b.data_assinatura === null) return -1;
    return b.data_assinatura.localeCompare(a.data_assinatura);
  });
}

export function ContratosTimeline({ contratos }: ContratosTimelineProps) {
  if (contratos.length === 0) {
    return (
      <p className="text-sm text-gray-500">Nenhum contrato para exibir.</p>
    );
  }

  const sorted = sortByDateDesc(contratos);

  return (
    <ol className="relative border-l border-gray-200">
      {sorted.map((contrato, index) => {
        const objeto = contrato.objeto
          ? truncate(contrato.objeto, MAX_OBJETO_LENGTH)
          : "Objeto não informado";

        return (
          <li key={index} className="mb-6 ml-6">
            {/* Timeline dot */}
            <span className="absolute -left-2 flex h-4 w-4 items-center justify-center rounded-full bg-white ring-2 ring-gray-200">
              <span className="h-2 w-2 rounded-full bg-blue-500" />
            </span>

            {/* Date header */}
            <time className="mb-1 block text-xs font-medium text-gray-500">
              {contrato.data_assinatura
                ? formatDate(contrato.data_assinatura)
                : "Data não informada"}
            </time>

            {/* Contract details card */}
            <div className="rounded-md border border-gray-100 bg-white px-3 py-2 shadow-sm">
              <p className="text-sm text-gray-800">{objeto}</p>
              <div className="mt-1 flex items-center gap-3">
                <span className="text-xs font-medium text-gray-500">
                  {contrato.orgao_codigo}
                </span>
                <span className="text-xs font-semibold text-gray-900">
                  {formatCurrency(contrato.valor)}
                </span>
              </div>
            </div>
          </li>
        );
      })}
    </ol>
  );
}
