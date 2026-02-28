// SecaoSocios — list of corporate partners with optional public servant badge.
//
// Design decisions:
// - The "servidor público" badge is visually prominent (orange) because it is
//   one of the primary alert triggers. Spotting the connection should require
//   zero inference on the reader's part.
// - orgao_lotacao is only shown when is_servidor_publico is true — it has no
//   meaning otherwise and would add noise to regular partner rows.
// - qualificacao is secondary information: shown in muted text below the name.

import { Card, CardHeader } from "@/components/ui/Card";
import { EmptyState } from "@/components/ui/EmptyState";
import { Badge } from "@/components/ui/Badge";
import type { SocioDTO } from "@/types/fornecedor";

interface SocioRowProps {
  socio: SocioDTO;
}

function SocioRow({ socio }: SocioRowProps) {
  return (
    <li className="flex items-start justify-between gap-4 py-2.5">
      <div className="min-w-0 flex-1">
        <p className="text-sm font-medium text-gray-900">{socio.nome}</p>
        {socio.qualificacao && (
          <p className="text-xs text-gray-500">{socio.qualificacao}</p>
        )}
        {socio.is_servidor_publico && socio.orgao_lotacao && (
          <p className="mt-0.5 text-xs text-orange-700">
            {socio.orgao_lotacao}
          </p>
        )}
      </div>

      {socio.is_servidor_publico && (
        <Badge className="shrink-0 bg-orange-100 text-orange-800">
          Servidor Público
        </Badge>
      )}
    </li>
  );
}

interface SecaoSociosProps {
  socios: SocioDTO[];
}

export function SecaoSocios({ socios }: SecaoSociosProps) {
  return (
    <Card>
      <CardHeader
        title="Quadro Societário"
        subtitle={`${socios.length} sócio${socios.length !== 1 ? "s" : ""}`}
      />

      {socios.length === 0 ? (
        <EmptyState message="Nenhum sócio registrado." />
      ) : (
        <ul className="divide-y divide-gray-100">
          {socios.map((socio, index) => (
            <SocioRow key={index} socio={socio} />
          ))}
        </ul>
      )}
    </Card>
  );
}
