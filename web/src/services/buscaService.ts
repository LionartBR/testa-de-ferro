import { apiFetch } from "./api";
import type { FornecedorResumo } from "@/types/fornecedor";

export function buscar(
  query: string,
  limit = 20,
  offset = 0,
): Promise<FornecedorResumo[]> {
  return apiFetch(
    `/busca?q=${encodeURIComponent(query)}&limit=${limit}&offset=${offset}`,
  );
}
