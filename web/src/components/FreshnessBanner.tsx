import type { Stats } from "@/types/stats";
import { formatDate } from "@/lib/formatters";

interface FreshnessBannerProps {
  fontes: Stats["fontes"];
}

export function FreshnessBanner({ fontes }: FreshnessBannerProps) {
  const entries = Object.entries(fontes);
  if (entries.length === 0) return null;

  return (
    <div className="rounded-md bg-blue-50 px-4 py-2 text-xs text-blue-700">
      <span className="font-medium">Dados:</span>{" "}
      {entries.map(([nome, meta], i) => (
        <span key={nome}>
          {nome}: {meta.ultima_atualizacao ? formatDate(meta.ultima_atualizacao) : "—"}
          {i < entries.length - 1 ? " | " : ""}
        </span>
      ))}
    </div>
  );
}
