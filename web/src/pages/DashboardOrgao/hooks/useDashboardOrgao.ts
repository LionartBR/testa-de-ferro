import { useCallback } from "react";
import { useParams } from "react-router-dom";
import { useApi } from "@/hooks/useApi";
import { getDashboard } from "@/services/orgaoService";
import type { DashboardOrgao } from "@/types/orgao";

export function useDashboardOrgao() {
  const { codigo } = useParams<{ codigo: string }>();

  const fetcher = useCallback((): Promise<DashboardOrgao> => {
    if (!codigo) return Promise.reject(new Error("Código do órgão ausente na rota"));
    return getDashboard(codigo);
  }, [codigo]);

  return useApi<DashboardOrgao>(codigo ? fetcher : null);
}
