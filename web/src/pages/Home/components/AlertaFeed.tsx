import type { AlertaFeedItem } from "@/types/alerta";
import { AlertaFeedItemCard } from "./AlertaFeedItem";
import { EmptyState } from "@/components/ui/EmptyState";

interface AlertaFeedProps {
  alertas: AlertaFeedItem[];
}

export function AlertaFeed({ alertas }: AlertaFeedProps) {
  if (alertas.length === 0) {
    return <EmptyState message="Nenhum alerta recente." />;
  }

  return (
    <div className="space-y-3">
      {alertas.map((alerta, i) => (
        <AlertaFeedItemCard key={`${alerta.cnpj}-${alerta.tipo}-${i}`} alerta={alerta} />
      ))}
    </div>
  );
}
