import { useCallback } from "react";
import { useApi } from "@/hooks/useApi";
import { getStats } from "@/services/statsService";
import type { Stats } from "@/types/stats";

export function useStats() {
  const fetcher = useCallback(() => getStats(), []);
  return useApi<Stats>(fetcher);
}
