export type TipoSancao = "CEIS" | "CNEP" | "CEPIM";

export interface Sancao {
  tipo: TipoSancao;
  orgao_sancionador: string;
  motivo: string;
  data_inicio: string;
  data_fim: string | null;
  vigente: boolean;
}
