import { Card } from "@/components/ui/Card";
import { SeveridadeBadge } from "@/components/SeveridadeBadge";
import { CNPJFormatado } from "@/components/CNPJFormatado";
import { ALERTA_LABELS } from "@/lib/constants";
import { formatDate } from "@/lib/formatters";
import type { AlertaFeedItem } from "@/types/alerta";

interface AlertaListaProps {
  alertas: AlertaFeedItem[];
}

export function AlertaLista({ alertas }: AlertaListaProps) {
  return (
    <ul className="space-y-3">
      {alertas.map((alerta, index) => (
        <li key={`${alerta.cnpj}-${alerta.tipo}-${index}`}>
          <Card>
            <div className="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
              <div className="flex items-center gap-2">
                <SeveridadeBadge severidade={alerta.severidade} />
                <span className="text-sm font-medium text-gray-900">
                  {ALERTA_LABELS[alerta.tipo]}
                </span>
              </div>
              <time className="text-xs text-gray-400 sm:flex-shrink-0">
                {formatDate(alerta.detectado_em)}
              </time>
            </div>

            <p className="mt-2 text-sm text-gray-700">{alerta.descricao}</p>

            {alerta.evidencia && (
              <p className="mt-1 text-xs text-gray-500 italic">
                {alerta.evidencia}
              </p>
            )}

            <div className="mt-3 flex flex-wrap items-center gap-x-4 gap-y-1 border-t border-gray-100 pt-2 text-xs text-gray-600">
              <CNPJFormatado cnpj={alerta.cnpj} />
              <span className="font-medium">{alerta.razao_social}</span>
              {alerta.socio_nome && (
                <span className="text-gray-500">Sócio: {alerta.socio_nome}</span>
              )}
            </div>
          </Card>
        </li>
      ))}
    </ul>
  );
}
