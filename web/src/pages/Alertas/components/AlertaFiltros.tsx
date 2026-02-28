import type { TipoAlerta, Severidade } from "@/types/alerta";
import { ALERTA_LABELS } from "@/lib/constants";

interface AlertaFiltrosProps {
  tipoAtual: TipoAlerta | null;
  severidadeAtual: Severidade | null;
  onTipoChange: (tipo: TipoAlerta | null) => void;
  onSeveridadeChange: (severidade: Severidade | null) => void;
}

const TIPO_ALERTA_OPTIONS: Array<{ value: TipoAlerta | ""; label: string }> = [
  { value: "", label: "Todos os tipos" },
  ...Object.entries(ALERTA_LABELS).map(([value, label]) => ({
    value: value as TipoAlerta,
    label,
  })),
];

const SEVERIDADE_OPTIONS: Array<{ value: Severidade | ""; label: string }> = [
  { value: "", label: "Todas as severidades" },
  { value: "GRAVISSIMO", label: "Gravíssimo" },
  { value: "GRAVE", label: "Grave" },
];

export function AlertaFiltros({
  tipoAtual,
  severidadeAtual,
  onTipoChange,
  onSeveridadeChange,
}: AlertaFiltrosProps) {
  return (
    <div className="flex flex-wrap items-center gap-4">
      <div className="flex items-center gap-2">
        <label htmlFor="filtro-tipo" className="text-sm font-medium text-gray-700">
          Tipo
        </label>
        <select
          id="filtro-tipo"
          value={tipoAtual ?? ""}
          onChange={(e) =>
            onTipoChange(e.target.value ? (e.target.value as TipoAlerta) : null)
          }
          className="rounded-md border border-gray-300 bg-white px-3 py-1.5 text-sm text-gray-700 shadow-sm focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
        >
          {TIPO_ALERTA_OPTIONS.map((option) => (
            <option key={option.value} value={option.value}>
              {option.label}
            </option>
          ))}
        </select>
      </div>

      <div className="flex items-center gap-2">
        <label
          htmlFor="filtro-severidade"
          className="text-sm font-medium text-gray-700"
        >
          Severidade
        </label>
        <select
          id="filtro-severidade"
          value={severidadeAtual ?? ""}
          onChange={(e) =>
            onSeveridadeChange(
              e.target.value ? (e.target.value as Severidade) : null,
            )
          }
          className="rounded-md border border-gray-300 bg-white px-3 py-1.5 text-sm text-gray-700 shadow-sm focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
        >
          {SEVERIDADE_OPTIONS.map((option) => (
            <option key={option.value} value={option.value}>
              {option.label}
            </option>
          ))}
        </select>
      </div>
    </div>
  );
}
