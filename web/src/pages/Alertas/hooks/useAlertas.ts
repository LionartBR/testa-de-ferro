import { useCallback } from "react";
import { useApi, type ApiState } from "@/hooks/useApi";
import { usePagination } from "@/hooks/usePagination";
import { getAlertas, getAlertasPorTipo } from "@/services/alertaService";
import type { AlertaFeedItem, TipoAlerta } from "@/types/alerta";

const PAGE_SIZE = 20;

type UseAlertasResult = ApiState<AlertaFeedItem[]> & {
  refetch: () => void;
  page: number;
  limit: number;
  nextPage: () => void;
  prevPage: () => void;
};

export function useAlertas(tipoFiltro: TipoAlerta | null): UseAlertasResult {
  const pagination = usePagination(PAGE_SIZE);

  const fetcher = useCallback(
    () =>
      tipoFiltro
        ? getAlertasPorTipo(tipoFiltro, pagination.limit, pagination.offset)
        : getAlertas(pagination.limit, pagination.offset),
    [tipoFiltro, pagination.limit, pagination.offset],
  );

  const apiState = useApi(fetcher);

  return {
    ...apiState,
    page: pagination.page,
    limit: pagination.limit,
    nextPage: pagination.nextPage,
    prevPage: pagination.prevPage,
  };
}
