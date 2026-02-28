import type { TipoAlerta } from "@/types/alerta";
import type { TipoIndicador } from "@/types/score";

export const ALERTA_LABELS: Record<TipoAlerta, string> = {
  SOCIO_SERVIDOR_PUBLICO: "Sócio é Servidor Público",
  RODIZIO_LICITACAO: "Rodízio de Licitação",
  EMPRESA_SANCIONADA_CONTRATANDO: "Empresa Sancionada Contratando",
  DOACAO_PARA_CONTRATANTE: "Doação para Contratante",
  SOCIO_SANCIONADO_EM_OUTRA: "Sócio Sancionado em Outra Empresa",
  TESTA_DE_FERRO: "Testa de Ferro",
};

export const INDICADOR_LABELS: Record<TipoIndicador, string> = {
  CAPITAL_SOCIAL_BAIXO: "Capital Social Baixo",
  EMPRESA_RECENTE: "Empresa Recente",
  CNAE_INCOMPATIVEL: "CNAE Incompatível",
  SOCIO_EM_MULTIPLAS_FORNECEDORAS: "Sócio em Múltiplas Fornecedoras",
  MESMO_ENDERECO: "Mesmo Endereço",
  FORNECEDOR_EXCLUSIVO: "Fornecedor Exclusivo",
  SEM_FUNCIONARIOS: "Sem Funcionários",
  CRESCIMENTO_SUBITO: "Crescimento Súbito",
  SANCAO_HISTORICA: "Sanção Histórica",
};

export const INDICADOR_PESOS: Record<TipoIndicador, number> = {
  CAPITAL_SOCIAL_BAIXO: 15,
  EMPRESA_RECENTE: 10,
  CNAE_INCOMPATIVEL: 10,
  SOCIO_EM_MULTIPLAS_FORNECEDORAS: 20,
  MESMO_ENDERECO: 15,
  FORNECEDOR_EXCLUSIVO: 10,
  SEM_FUNCIONARIOS: 10,
  CRESCIMENTO_SUBITO: 10,
  SANCAO_HISTORICA: 5,
};
