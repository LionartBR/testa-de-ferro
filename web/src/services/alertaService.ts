import { apiFetch } from "./api";
import type { AlertaFeedItem, TipoAlerta } from "@/types/alerta";

export function getAlertas(
  limit = 20,
  offset = 0,
): Promise<AlertaFeedItem[]> {
  return apiFetch(`/alertas?limit=${limit}&offset=${offset}`);
}

export function getAlertasPorTipo(
  tipo: TipoAlerta,
  limit = 20,
  offset = 0,
): Promise<AlertaFeedItem[]> {
  return apiFetch(`/alertas/${tipo}?limit=${limit}&offset=${offset}`);
}
