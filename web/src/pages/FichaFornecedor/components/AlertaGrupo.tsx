// AlertaGrupo — expandable group of alerts sharing the same TipoAlerta.
//
// Design decisions:
// - Uses native <details>/<summary> for expand/collapse. Zero JS state,
//   zero re-renders, accessible by default. The browser handles the toggle
//   animation via CSS transition on max-height when supported.
// - The header shows the alert label + count badge + the worst severidade
//   badge in the group so the reader can triage without expanding.
// - Each item in the group shows its descricao and evidencia. Evidencia is
//   displayed in a muted monospace block to visually distinguish raw data
//   from the human-readable description.

import { SeveridadeBadge } from "@/components/SeveridadeBadge";
import { ALERTA_LABELS } from "@/lib/constants";
import type { AlertaGroup } from "../types";
import type { Severidade } from "@/types/alerta";

function worstSeveridade(severidades: Severidade[]): Severidade {
  return severidades.includes("GRAVISSIMO") ? "GRAVISSIMO" : "GRAVE";
}

interface AlertaGrupoProps {
  group: AlertaGroup;
}

export function AlertaGrupo({ group }: AlertaGrupoProps) {
  const { tipo, alertas } = group;
  const worst = worstSeveridade(alertas.map((a) => a.severidade));
  const label = ALERTA_LABELS[tipo];

  return (
    <details className="group rounded-md border border-gray-200 bg-white">
      <summary className="flex cursor-pointer select-none list-none items-center gap-3 px-4 py-3 hover:bg-gray-50">
        {/* Custom disclosure triangle via CSS rotate */}
        <span
          className="inline-block text-gray-400 transition-transform group-open:rotate-90"
          aria-hidden="true"
        >
          ▶
        </span>

        <span className="flex-1 text-sm font-medium text-gray-900">
          {label}
        </span>

        <span className="rounded-full bg-gray-100 px-2 py-0.5 text-xs font-semibold text-gray-700">
          {alertas.length}
        </span>

        <SeveridadeBadge severidade={worst} />
      </summary>

      <ul className="divide-y divide-gray-100 border-t border-gray-200">
        {alertas.map((alerta, index) => (
          <li key={index} className="px-4 py-3">
            <p className="text-sm text-gray-800">{alerta.descricao}</p>
            {alerta.evidencia && (
              <p className="mt-1 font-mono text-xs text-gray-500">
                {alerta.evidencia}
              </p>
            )}
          </li>
        ))}
      </ul>
    </details>
  );
}
