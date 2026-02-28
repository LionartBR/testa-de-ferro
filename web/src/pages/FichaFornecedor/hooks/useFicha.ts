// useFicha — fetches the complete dossier for a single supplier.
//
// Design decisions:
// - The fetcher is stabilized with useCallback so that useApi's internal
//   useEffect only re-runs when the cnpj actually changes, not on every
//   parent re-render.
// - Returns the full ApiState<FichaCompleta> union so the caller can
//   discriminate on status without additional state.

import { useCallback } from "react";
import { useApi } from "@/hooks/useApi";
import { getFicha } from "@/services/fornecedorService";
import type { FichaCompleta } from "@/types/fornecedor";
import type { ApiState } from "@/hooks/useApi";

export function useFicha(cnpj: string): ApiState<FichaCompleta> & { refetch: () => void } {
  const fetcher = useCallback(() => getFicha(cnpj), [cnpj]);
  return useApi(fetcher);
}
