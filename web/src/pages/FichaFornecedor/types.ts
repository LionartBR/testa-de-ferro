// Local types for FichaFornecedor page.
//
// Most types are defined globally in @/types/*. This file only captures
// derived structures needed for internal view logic — groupings and display
// states that do not belong in the domain layer.

import type { AlertaCritico, TipoAlerta } from "@/types/alerta";

// Alertas of the same TipoAlerta grouped together for the expandable UI.
// The count is pre-computed so the header renders without iterating each time.
export interface AlertaGroup {
  tipo: TipoAlerta;
  alertas: AlertaCritico[];
}
