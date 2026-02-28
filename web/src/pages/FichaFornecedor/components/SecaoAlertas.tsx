// SecaoAlertas — renders all critical alerts grouped by type.
//
// Design decisions:
// - Grouping happens here at render time (useMemo). It is a pure
//   transformation of the props array — no state, no side effects.
// - The section header shows the total alert count so the reader gets an
//   immediate severity signal before opening any group.
// - When there are no alerts the section renders an EmptyState instead of
//   hiding itself: the absence of alerts is meaningful information on a
//   risk dossier.

import { useMemo } from "react";
import { Card, CardHeader } from "@/components/ui/Card";
import { EmptyState } from "@/components/ui/EmptyState";
import { Badge } from "@/components/ui/Badge";
import { AlertaGrupo } from "./AlertaGrupo";
import type { AlertaCritico, TipoAlerta } from "@/types/alerta";
import type { AlertaGroup } from "../types";

function groupAlerts(alertas: AlertaCritico[]): AlertaGroup[] {
  const map = new Map<TipoAlerta, AlertaCritico[]>();

  for (const alerta of alertas) {
    const existing = map.get(alerta.tipo);
    if (existing) {
      existing.push(alerta);
    } else {
      map.set(alerta.tipo, [alerta]);
    }
  }

  return Array.from(map.entries()).map(([tipo, items]) => ({ tipo, alertas: items }));
}

interface SecaoAlertasProps {
  alertas: AlertaCritico[];
}

export function SecaoAlertas({ alertas }: SecaoAlertasProps) {
  const groups = useMemo(() => groupAlerts(alertas), [alertas]);

  const countBadge =
    alertas.length > 0 ? (
      <Badge className="bg-red-100 text-red-800">{alertas.length}</Badge>
    ) : null;

  return (
    <Card>
      <CardHeader title="Alertas Críticos" action={countBadge} />

      {groups.length === 0 ? (
        <EmptyState message="Nenhum alerta crítico identificado." />
      ) : (
        <div className="space-y-2">
          {groups.map((group) => (
            <AlertaGrupo key={group.tipo} group={group} />
          ))}
        </div>
      )}
    </Card>
  );
}
