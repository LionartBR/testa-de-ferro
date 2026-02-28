import { useCallback } from "react";
import { useApi, type ApiState } from "@/hooks/useApi";
import { useDebounce } from "@/hooks/useDebounce";
import { buscar } from "@/services/buscaService";
import type { FornecedorResumo } from "@/types/fornecedor";

const DEBOUNCE_MS = 300;
const MIN_QUERY_LENGTH = 2;
const RESULT_LIMIT = 20;

type UseBuscaResult = ApiState<FornecedorResumo[]> & { refetch: () => void };

export function useBusca(query: string): UseBuscaResult {
  const debouncedQuery = useDebounce(query, DEBOUNCE_MS);

  const fetcher = useCallback(
    () => buscar(debouncedQuery, RESULT_LIMIT, 0),
    [debouncedQuery],
  );

  const isQueryLongEnough = debouncedQuery.length >= MIN_QUERY_LENGTH;

  return useApi<FornecedorResumo[]>(isQueryLongEnough ? fetcher : null);
}
