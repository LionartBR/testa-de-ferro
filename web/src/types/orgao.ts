export interface Orgao {
  nome: string;
  sigla: string | null;
  codigo: string;
}

export interface TopFornecedorOrgao {
  cnpj: string;
  razao_social: string;
  score_risco: number;
  valor_total: string;
  qtd_contratos: number;
}

export interface DashboardOrgao {
  orgao: Orgao;
  qtd_contratos: number;
  total_contratado: string;
  qtd_fornecedores: number;
  top_fornecedores: TopFornecedorOrgao[];
}
