import type { AlertaCritico } from "./alerta";
import type { ContratoResumo } from "./contrato";
import type { Doacao } from "./doacao";
import type { Sancao } from "./sancao";
import type { Score, FaixaRisco } from "./score";

export type SituacaoCadastral =
  | "ATIVA"
  | "SUSPENSA"
  | "INAPTA"
  | "BAIXADA"
  | "NULA";

export interface Endereco {
  logradouro: string;
  municipio: string;
  uf: string;
  cep: string;
}

export interface SocioDTO {
  nome: string;
  qualificacao: string | null;
  is_servidor_publico: boolean;
  orgao_lotacao: string | null;
}

export interface FornecedorResumo {
  cnpj: string;
  razao_social: string;
  situacao: SituacaoCadastral;
  score_risco: number;
  faixa_risco: FaixaRisco;
  qtd_alertas: number;
  max_severidade: string | null;
  total_contratos: number;
  valor_total: string;
}

export interface FichaCompleta {
  cnpj: string;
  razao_social: string;
  situacao: SituacaoCadastral;
  data_abertura: string | null;
  capital_social: string | null;
  cnae_principal: string | null;
  cnae_descricao: string | null;
  endereco: Endereco | null;
  total_contratos: number;
  valor_total_contratos: string;
  alertas_criticos: AlertaCritico[];
  score: Score | null;
  socios: SocioDTO[];
  sancoes: Sancao[];
  contratos: ContratoResumo[];
  doacoes: Doacao[];
  disclaimer: string;
}
