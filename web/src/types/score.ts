export type TipoIndicador =
  | "CAPITAL_SOCIAL_BAIXO"
  | "EMPRESA_RECENTE"
  | "CNAE_INCOMPATIVEL"
  | "SOCIO_EM_MULTIPLAS_FORNECEDORAS"
  | "MESMO_ENDERECO"
  | "FORNECEDOR_EXCLUSIVO"
  | "SEM_FUNCIONARIOS"
  | "CRESCIMENTO_SUBITO"
  | "SANCAO_HISTORICA";

export type FaixaRisco = "Baixo" | "Moderado" | "Alto" | "Critico";

export interface Indicador {
  tipo: TipoIndicador;
  peso: number;
  descricao: string;
  evidencia: string;
}

export interface Score {
  valor: number;
  faixa: FaixaRisco;
  indicadores: Indicador[];
}
