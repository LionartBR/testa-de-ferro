export type TipoAlerta =
  | "SOCIO_SERVIDOR_PUBLICO"
  | "RODIZIO_LICITACAO"
  | "EMPRESA_SANCIONADA_CONTRATANDO"
  | "DOACAO_PARA_CONTRATANTE"
  | "SOCIO_SANCIONADO_EM_OUTRA"
  | "TESTA_DE_FERRO";

export type Severidade = "GRAVE" | "GRAVISSIMO";

export interface AlertaCritico {
  tipo: TipoAlerta;
  severidade: Severidade;
  descricao: string;
  evidencia: string;
}

export interface AlertaFeedItem {
  tipo: TipoAlerta;
  severidade: Severidade;
  descricao: string;
  evidencia: string;
  detectado_em: string;
  cnpj: string;
  razao_social: string;
  socio_nome: string | null;
}
