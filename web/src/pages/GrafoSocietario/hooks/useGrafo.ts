import { useCallback } from "react";
import { useParams } from "react-router-dom";
import { useApi } from "@/hooks/useApi";
import { getGrafo } from "@/services/fornecedorService";
import type { Grafo } from "@/types/grafo";

export function useGrafo() {
  const { cnpj } = useParams<{ cnpj: string }>();

  const fetcher = useCallback((): Promise<Grafo> => {
    if (!cnpj) return Promise.reject(new Error("CNPJ ausente na rota"));
    return getGrafo(cnpj);
  }, [cnpj]);

  return useApi<Grafo>(cnpj ? fetcher : null);
}
