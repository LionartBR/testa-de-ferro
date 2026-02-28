import type { FaixaRisco } from "@/types/score";
import type { Severidade } from "@/types/alerta";

export const FAIXA_COLORS: Record<FaixaRisco, string> = {
  Baixo: "bg-green-100 text-green-800",
  Moderado: "bg-yellow-100 text-yellow-800",
  Alto: "bg-orange-100 text-orange-800",
  Critico: "bg-red-100 text-red-800",
};

export const FAIXA_BG: Record<FaixaRisco, string> = {
  Baixo: "#22c55e",
  Moderado: "#eab308",
  Alto: "#f97316",
  Critico: "#ef4444",
};

export const SEVERIDADE_COLORS: Record<Severidade, string> = {
  GRAVE: "bg-orange-100 text-orange-800 border-orange-300",
  GRAVISSIMO: "bg-red-100 text-red-800 border-red-300",
};
