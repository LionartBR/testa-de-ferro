import type { TipoAlerta, Severidade } from "@/types/alerta";
import { ALERTA_LABELS } from "@/lib/constants";
import { Badge } from "@/components/ui/Badge";
import { SEVERIDADE_COLORS } from "@/lib/colors";

// Condition descriptions are editorial content describing detection criteria.
// Changes here require review by the methodology team.
const ALERTA_CONDICOES: Record<TipoAlerta, string> = {
  SOCIO_SERVIDOR_PUBLICO:
    "Sócio identificado como servidor público federal ativo. Match por nome completo + dígitos visíveis do CPF mascarado do Portal da Transparência.",
  RODIZIO_LICITACAO:
    "Padrão de alternância entre empresas do mesmo grupo societário em licitações do mesmo órgão, sugerindo conluio para burlar regras de participação.",
  EMPRESA_SANCIONADA_CONTRATANDO:
    "Empresa com sanção vigente no CEIS ou CNEP (data_fim NULL ou futura) que continua celebrando contratos com o governo.",
  DOACAO_PARA_CONTRATANTE:
    "Sócio ou empresa realizou doação eleitoral acima de R$ 10.000 para candidato vinculado ao órgão contratante, e o contrato supera R$ 500.000.",
  SOCIO_SANCIONADO_EM_OUTRA:
    "Sócio figura como responsável em outra empresa que possui sanção vigente, mesmo que a empresa atual esteja em situação regular.",
  TESTA_DE_FERRO:
    "Combinação de múltiplos indicadores de alta confiança que caracteriza perfil de empresa fictícia usada para ocultar o real beneficiário.",
};

const ALERTA_SEVERIDADES: Record<TipoAlerta, Severidade> = {
  SOCIO_SERVIDOR_PUBLICO: "GRAVISSIMO",
  RODIZIO_LICITACAO: "GRAVE",
  EMPRESA_SANCIONADA_CONTRATANDO: "GRAVISSIMO",
  DOACAO_PARA_CONTRATANTE: "GRAVE",
  SOCIO_SANCIONADO_EM_OUTRA: "GRAVE",
  TESTA_DE_FERRO: "GRAVISSIMO",
};

const ALERTAS: TipoAlerta[] = [
  "SOCIO_SERVIDOR_PUBLICO",
  "EMPRESA_SANCIONADA_CONTRATANDO",
  "TESTA_DE_FERRO",
  "RODIZIO_LICITACAO",
  "DOACAO_PARA_CONTRATANTE",
  "SOCIO_SANCIONADO_EM_OUTRA",
];

export function ExplicacaoAlertas() {
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full divide-y divide-gray-200">
        <thead className="bg-gray-50">
          <tr>
            <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">
              Alerta
            </th>
            <th className="px-4 py-3 text-center text-xs font-medium uppercase tracking-wider text-gray-500 w-32">
              Severidade
            </th>
            <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">
              Condição de detecção
            </th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-200 bg-white">
          {ALERTAS.map((tipo) => {
            const severidade = ALERTA_SEVERIDADES[tipo];
            return (
              <tr key={tipo} className="hover:bg-gray-50">
                <td className="whitespace-nowrap px-4 py-3 text-sm font-medium text-gray-900">
                  {ALERTA_LABELS[tipo]}
                </td>
                <td className="px-4 py-3 text-center">
                  <Badge className={SEVERIDADE_COLORS[severidade]}>
                    {severidade === "GRAVISSIMO" ? "Gravíssimo" : "Grave"}
                  </Badge>
                </td>
                <td className="px-4 py-3 text-sm text-gray-600">
                  {ALERTA_CONDICOES[tipo]}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
      <p className="mt-3 text-xs text-gray-400">
        Alertas são dimensões independentes do score. Um fornecedor com score
        zero pode ter alertas gravíssimos — e vice-versa.
      </p>
    </div>
  );
}
