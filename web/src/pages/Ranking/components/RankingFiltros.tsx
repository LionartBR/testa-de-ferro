import type { FaixaRisco } from "@/types/score";

interface RankingFiltrosProps {
  faixaAtual: FaixaRisco | "";
  onFaixaChange: (faixa: FaixaRisco | "") => void;
}

const FAIXA_OPTIONS: { value: FaixaRisco | ""; label: string }[] = [
  { value: "", label: "Todas as faixas" },
  { value: "Critico", label: "Crítico" },
  { value: "Alto", label: "Alto" },
  { value: "Moderado", label: "Moderado" },
  { value: "Baixo", label: "Baixo" },
];

export function RankingFiltros({ faixaAtual, onFaixaChange }: RankingFiltrosProps) {
  return (
    <div className="flex items-center gap-3">
      <label
        htmlFor="filtro-faixa"
        className="text-sm font-medium text-gray-700"
      >
        Faixa de risco
      </label>
      <select
        id="filtro-faixa"
        value={faixaAtual}
        onChange={(e) => onFaixaChange(e.target.value as FaixaRisco | "")}
        className="rounded-md border border-gray-300 bg-white px-3 py-1.5 text-sm text-gray-700 shadow-sm focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
      >
        {FAIXA_OPTIONS.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
    </div>
  );
}
