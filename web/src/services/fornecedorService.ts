import { apiFetch, apiFetchBlob } from "./api";
import { cnpjToParam } from "@/lib/formatters";
import type { FornecedorResumo, FichaCompleta } from "@/types/fornecedor";
import type { Grafo } from "@/types/grafo";

export function getFicha(cnpj: string): Promise<FichaCompleta> {
  return apiFetch(`/fornecedores/${cnpjToParam(cnpj)}`);
}

export function getRanking(
  limit = 20,
  offset = 0,
): Promise<FornecedorResumo[]> {
  return apiFetch(`/fornecedores/ranking?limit=${limit}&offset=${offset}`);
}

export function getGrafo(cnpj: string): Promise<Grafo> {
  return apiFetch(`/fornecedores/${cnpjToParam(cnpj)}/grafo`);
}

export type FormatoExport = "csv" | "json" | "pdf";

export async function exportar(
  cnpj: string,
  formato: FormatoExport,
): Promise<Blob> {
  return apiFetchBlob(
    `/fornecedores/${cnpjToParam(cnpj)}/export?formato=${formato}`,
  );
}
