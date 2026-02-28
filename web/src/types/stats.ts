export interface FonteMetadata {
  ultima_atualizacao: string | null;
  registros: number;
}

export interface Stats {
  total_fornecedores: number;
  total_contratos: number;
  total_alertas: number;
  fontes: Record<string, FonteMetadata>;
}
