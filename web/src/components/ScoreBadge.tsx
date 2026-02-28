import type { FaixaRisco } from "@/types/score";
import { Badge } from "./ui/Badge";
import { FAIXA_COLORS } from "@/lib/colors";

interface ScoreBadgeProps {
  valor: number;
  faixa: FaixaRisco;
}

export function ScoreBadge({ valor, faixa }: ScoreBadgeProps) {
  return (
    <Badge className={FAIXA_COLORS[faixa]}>
      {valor} — {faixa}
    </Badge>
  );
}
