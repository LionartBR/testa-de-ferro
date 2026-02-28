import type { Severidade } from "@/types/alerta";
import { Badge } from "./ui/Badge";
import { SEVERIDADE_COLORS } from "@/lib/colors";

interface SeveridadeBadgeProps {
  severidade: Severidade;
}

export function SeveridadeBadge({ severidade }: SeveridadeBadgeProps) {
  return (
    <Badge className={`border ${SEVERIDADE_COLORS[severidade]}`}>
      {severidade}
    </Badge>
  );
}
