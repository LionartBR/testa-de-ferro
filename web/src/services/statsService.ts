import { apiFetch } from "./api";
import type { Stats } from "@/types/stats";

export function getStats(): Promise<Stats> {
  return apiFetch("/stats");
}
