import type { AlertaFeedItem as AlertaFeedItemType } from "@/types/alerta";
import { Card } from "@/components/ui/Card";
import { SeveridadeBadge } from "@/components/SeveridadeBadge";
import { CNPJFormatado } from "@/components/CNPJFormatado";
import { ALERTA_LABELS } from "@/lib/constants";
import { formatDate } from "@/lib/formatters";

interface AlertaFeedItemProps {
  alerta: AlertaFeedItemType;
}

export function AlertaFeedItemCard({ alerta }: AlertaFeedItemProps) {
  return (
    <Card className="p-3">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2">
            <SeveridadeBadge severidade={alerta.severidade} />
            <span className="text-xs font-medium text-gray-700">
              {ALERTA_LABELS[alerta.tipo]}
            </span>
          </div>
          <p className="mt-1.5 text-sm text-gray-600">{alerta.descricao}</p>
          <div className="mt-2 flex items-center gap-3 text-xs text-gray-500">
            <CNPJFormatado cnpj={alerta.cnpj} />
            <span>{alerta.razao_social}</span>
            {alerta.socio_nome && (
              <span className="italic">{alerta.socio_nome}</span>
            )}
          </div>
        </div>
        <span className="shrink-0 text-xs text-gray-400">
          {formatDate(alerta.detectado_em)}
        </span>
      </div>
    </Card>
  );
}
