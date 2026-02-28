// Local types for graph display state.
// GrafoNo and GrafoAresta live in @/types/grafo — these types
// represent UI-layer concerns only (filtering, selection state).

export interface GrafoDisplayFilters {
  showEmpresas: boolean;
  showSocios: boolean;
}
