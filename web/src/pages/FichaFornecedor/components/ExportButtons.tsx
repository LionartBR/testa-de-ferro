// ExportButtons — three export triggers (CSV / JSON / PDF).
//
// Design decisions:
// - Each button is independent: pressing "CSV" while "PDF" is loading is
//   allowed. We track loading state per format via a Set instead of a single
//   boolean to support concurrent requests.
// - Error message is transient: shown below the buttons, auto-clears on next
//   successful download to avoid stale errors confusing the user.
// - Buttons are secondary variant to avoid competing visually with the
//   primary call-to-action content on the page.

import { useState, useCallback } from "react";
import { Button } from "@/components/ui/Button";
import { exportar } from "@/services/fornecedorService";
import type { FormatoExport } from "@/services/fornecedorService";

const FORMATS: { formato: FormatoExport; label: string }[] = [
  { formato: "csv", label: "CSV" },
  { formato: "json", label: "JSON" },
  { formato: "pdf", label: "PDF" },
];

interface ExportButtonsProps {
  cnpj: string;
}

export function ExportButtons({ cnpj }: ExportButtonsProps) {
  const [loading, setLoading] = useState<Set<FormatoExport>>(new Set());
  const [error, setError] = useState<string | null>(null);

  const handleExport = useCallback(
    (formato: FormatoExport) => {
      setLoading((prev) => new Set(prev).add(formato));
      setError(null);

      exportar(cnpj, formato).then(
        (blob) => {
          const url = URL.createObjectURL(blob);
          const anchor = document.createElement("a");
          anchor.href = url;
          anchor.download = `ficha-${cnpj}.${formato}`;
          document.body.appendChild(anchor);
          anchor.click();
          document.body.removeChild(anchor);
          URL.revokeObjectURL(url);

          setLoading((prev) => {
            const next = new Set(prev);
            next.delete(formato);
            return next;
          });
        },
        (err: unknown) => {
          const message =
            err instanceof Error ? err.message : "Erro ao exportar";
          setError(message);
          setLoading((prev) => {
            const next = new Set(prev);
            next.delete(formato);
            return next;
          });
        },
      );
    },
    [cnpj],
  );

  return (
    <div className="flex flex-col gap-2">
      <div className="flex flex-wrap gap-2">
        {FORMATS.map(({ formato, label }) => (
          <Button
            key={formato}
            variant="secondary"
            size="sm"
            disabled={loading.has(formato)}
            onClick={() => handleExport(formato)}
          >
            {loading.has(formato) ? "Exportando…" : `Exportar ${label}`}
          </Button>
        ))}
      </div>

      {error && (
        <p className="text-xs text-red-600">{error}</p>
      )}
    </div>
  );
}
