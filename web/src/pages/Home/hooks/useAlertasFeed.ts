import { useCallback } from "react";
import { useApi } from "@/hooks/useApi";
import { usePagination } from "@/hooks/usePagination";
import { getAlertas } from "@/services/alertaService";
import type { AlertaFeedItem } from "@/types/alerta";

export function useAlertasFeed() {
  const pagination = usePagination(10);

  const fetcher = useCallback(
    () => getAlertas(pagination.limit, pagination.offset),
    [pagination.limit, pagination.offset],
  );

  const state = useApi<AlertaFeedItem[]>(fetcher);

  return { ...state, pagination };
}
