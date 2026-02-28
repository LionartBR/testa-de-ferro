import type { GrafoNo } from "@/types/grafo";

interface NoTooltipProps {
  no: GrafoNo | null;
}

// Displays details for a selected node.
// Rendered as an inline panel rather than a floating tooltip so it
// remains accessible and visible without hover state.
export function NoTooltip({ no }: NoTooltipProps) {
  if (!no) {
    return (
      <p className="text-xs text-gray-400">
        Clique em um nó para ver detalhes.
      </p>
    );
  }

  const tipoLabel = no.tipo === "empresa" ? "Empresa" : "Sócio";

  return (
    <div className="space-y-2">
      <div>
        <span className="text-xs font-medium uppercase tracking-wide text-gray-400">
          {tipoLabel}
        </span>
        <p className="mt-0.5 text-sm font-semibold text-gray-900 break-words">
          {no.label}
        </p>
      </div>

      {no.score !== null && (
        <div>
          <span className="text-xs text-gray-500">Score de risco</span>
          <p className="text-sm font-medium text-gray-800">{no.score}</p>
        </div>
      )}

      {no.qtd_alertas !== null && (
        <div>
          <span className="text-xs text-gray-500">Alertas críticos</span>
          <p className="text-sm font-medium text-gray-800">
            {no.qtd_alertas === 0 ? "Nenhum" : no.qtd_alertas}
          </p>
        </div>
      )}
    </div>
  );
}
