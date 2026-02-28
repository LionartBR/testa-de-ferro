import type { TipoAlerta, Severidade } from "@/types/alerta";
import { Badge } from "./ui/Badge";
import { SEVERIDADE_COLORS } from "@/lib/colors";
import { ALERTA_LABELS } from "@/lib/constants";

interface AlertaCriticoBadgeProps {
  tipo: TipoAlerta;
  severidade: Severidade;
}

export function AlertaCriticoBadge({ tipo, severidade }: AlertaCriticoBadgeProps) {
  return (
    <Badge className={`border ${SEVERIDADE_COLORS[severidade]}`}>
      {ALERTA_LABELS[tipo]}
    </Badge>
  );
}
