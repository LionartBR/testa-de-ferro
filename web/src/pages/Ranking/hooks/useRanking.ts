import { useCallback } from "react";
import { useApi, type ApiState } from "@/hooks/useApi";
import { usePagination } from "@/hooks/usePagination";
import { getRanking } from "@/services/fornecedorService";
import type { FornecedorResumo } from "@/types/fornecedor";

const PAGE_SIZE = 20;

type UseRankingResult = ApiState<FornecedorResumo[]> & {
  refetch: () => void;
  page: number;
  limit: number;
  offset: number;
  nextPage: () => void;
  prevPage: () => void;
};

export function useRanking(): UseRankingResult {
  const pagination = usePagination(PAGE_SIZE);

  const fetcher = useCallback(
    () => getRanking(pagination.limit, pagination.offset),
    [pagination.limit, pagination.offset],
  );

  const apiState = useApi(fetcher);

  return {
    ...apiState,
    page: pagination.page,
    limit: pagination.limit,
    offset: pagination.offset,
    nextPage: pagination.nextPage,
    prevPage: pagination.prevPage,
  };
}
