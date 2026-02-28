import { apiFetch } from "./api";
import type { ContratoResumo } from "@/types/contrato";

export function getContratos(
  params: {
    cnpj?: string;
    orgao_codigo?: string;
    limit?: number;
    offset?: number;
  } = {},
): Promise<ContratoResumo[]> {
  const searchParams = new URLSearchParams();
  if (params.cnpj) searchParams.set("cnpj", params.cnpj);
  if (params.orgao_codigo) searchParams.set("orgao_codigo", params.orgao_codigo);
  if (params.limit != null) searchParams.set("limit", String(params.limit));
  if (params.offset != null) searchParams.set("offset", String(params.offset));

  const qs = searchParams.toString();
  return apiFetch(`/contratos${qs ? `?${qs}` : ""}`);
}
