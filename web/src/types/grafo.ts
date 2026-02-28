export type TipoNo = "empresa" | "socio";

export interface GrafoNo {
  id: string;
  tipo: TipoNo;
  label: string;
  score: number | null;
  qtd_alertas: number | null;
}

export interface GrafoAresta {
  source: string;
  target: string;
  tipo: string;
  label: string | null;
}

export interface Grafo {
  nos: GrafoNo[];
  arestas: GrafoAresta[];
  truncado: boolean;
}
