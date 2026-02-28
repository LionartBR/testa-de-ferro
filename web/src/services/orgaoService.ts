import { apiFetch } from "./api";
import type { DashboardOrgao } from "@/types/orgao";

export function getDashboard(codigo: string): Promise<DashboardOrgao> {
  return apiFetch(`/orgaos/${encodeURIComponent(codigo)}/dashboard`);
}
