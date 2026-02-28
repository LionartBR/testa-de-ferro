// useExport — triggers a file download for CSV / JSON / PDF exports.
//
// Design decisions:
// - Export is imperative (user-initiated), not declarative. We do NOT use
//   useApi here because there is no persistent loading state to display —
//   the button shows a transient spinner while the request is in-flight.
// - Blob → object URL → anchor click is the only cross-browser reliable
//   pattern for programmatic downloads without opening a new tab.
// - The object URL is revoked immediately after click to avoid memory leaks.
// - filename is derived from cnpj + format so the browser's download dialog
//   shows a meaningful name instead of a generic UUID.

import { useState, useCallback } from "react";
import { exportar } from "@/services/fornecedorService";
import type { FormatoExport } from "@/services/fornecedorService";

interface ExportState {
  loading: boolean;
  error: string | null;
}

interface UseExportReturn extends ExportState {
  download: (formato: FormatoExport) => void;
}

export function useExport(cnpj: string): UseExportReturn {
  const [state, setState] = useState<ExportState>({
    loading: false,
    error: null,
  });

  const download = useCallback(
    (formato: FormatoExport) => {
      setState({ loading: true, error: null });

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
          setState({ loading: false, error: null });
        },
        (err: unknown) => {
          const message =
            err instanceof Error ? err.message : "Erro ao exportar";
          setState({ loading: false, error: message });
        },
      );
    },
    [cnpj],
  );

  return { ...state, download };
}
