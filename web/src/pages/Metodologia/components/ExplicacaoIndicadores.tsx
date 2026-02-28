import type { TipoIndicador } from "@/types/score";
import { INDICADOR_LABELS, INDICADOR_PESOS } from "@/lib/constants";

// Brief descriptions for each cumulative risk indicator.
// These are intentionally hardcoded — they are editorial content
// that describes the methodology and must be reviewed before changing.
const INDICADOR_DESCRICOES: Record<TipoIndicador, string> = {
  CAPITAL_SOCIAL_BAIXO:
    "Capital declarado insuficiente para o porte dos contratos celebrados, cruzado com CNAE para evitar falsos positivos em empresas de serviço.",
  EMPRESA_RECENTE:
    "Empresa constituída há menos de 2 anos que já celebra contratos de alto valor, padrão associado a empresas criadas oportunisticamente.",
  CNAE_INCOMPATIVEL:
    "Atividade econômica declarada (CNAE) incompatível com o objeto dos contratos firmados com o governo.",
  SOCIO_EM_MULTIPLAS_FORNECEDORAS:
    "Mesmo sócio figura como responsável em duas ou mais empresas fornecedoras do mesmo órgão contratante.",
  MESMO_ENDERECO:
    "Duas ou mais empresas fornecedoras registradas no mesmo logradouro e número, sugestivo de endereço fictício compartilhado.",
  FORNECEDOR_EXCLUSIVO:
    "Empresa contrata exclusivamente com um único órgão público, sem atividade comercial privada aparente.",
  SEM_FUNCIONARIOS:
    "Empresa sem registro de vínculos empregatícios ativo no CAGED/e-Social para o porte declarado.",
  CRESCIMENTO_SUBITO:
    "Aumento abrupto no volume de contratos ou faturamento sem histórico comercial prévio que o justifique.",
  SANCAO_HISTORICA:
    "Empresa apresenta sanção administrativa já expirada (CEIS/CNEP). Não gera alerta crítico, mas contribui com peso 5 no score.",
};

const INDICADORES: TipoIndicador[] = [
  "CAPITAL_SOCIAL_BAIXO",
  "EMPRESA_RECENTE",
  "CNAE_INCOMPATIVEL",
  "SOCIO_EM_MULTIPLAS_FORNECEDORAS",
  "MESMO_ENDERECO",
  "FORNECEDOR_EXCLUSIVO",
  "SEM_FUNCIONARIOS",
  "CRESCIMENTO_SUBITO",
  "SANCAO_HISTORICA",
];

export function ExplicacaoIndicadores() {
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full divide-y divide-gray-200">
        <thead className="bg-gray-50">
          <tr>
            <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">
              Indicador
            </th>
            <th className="px-4 py-3 text-center text-xs font-medium uppercase tracking-wider text-gray-500 w-20">
              Peso
            </th>
            <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">
              Descrição
            </th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-200 bg-white">
          {INDICADORES.map((tipo) => (
            <tr key={tipo} className="hover:bg-gray-50">
              <td className="whitespace-nowrap px-4 py-3 text-sm font-medium text-gray-900">
                {INDICADOR_LABELS[tipo]}
              </td>
              <td className="px-4 py-3 text-center text-sm tabular-nums font-semibold text-gray-700">
                {INDICADOR_PESOS[tipo]}
              </td>
              <td className="px-4 py-3 text-sm text-gray-600">
                {INDICADOR_DESCRICOES[tipo]}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="mt-3 text-xs text-gray-400">
        Score máximo teórico: 105 pontos (soma de todos os pesos). Na prática,
        um fornecedor raramente acumula todos os indicadores simultaneamente.
      </p>
    </div>
  );
}
