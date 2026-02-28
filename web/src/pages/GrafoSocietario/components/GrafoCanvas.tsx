// PLACEHOLDER — SVG/div-based graph representation.
// Will be replaced with a force-directed layout (e.g., d3-force or react-force-graph)
// once the library is installed. The current implementation provides a functional
// scrollable node grid so the feature is testable end-to-end without external deps.

import { useState } from "react";
import type { Grafo, GrafoNo, GrafoAresta } from "@/types/grafo";
import { NoTooltip } from "./NoTooltip";
import { EmptyState } from "@/components/ui/EmptyState";

interface GrafoCanvasProps {
  grafo: Grafo;
}

function nodeColorClasses(tipo: GrafoNo["tipo"]): string {
  return tipo === "empresa"
    ? "bg-blue-100 border-blue-400 text-blue-900"
    : "bg-green-100 border-green-400 text-green-900";
}

function edgeLabel(aresta: GrafoAresta): string {
  return aresta.label ?? aresta.tipo;
}

export function GrafoCanvas({ grafo }: GrafoCanvasProps) {
  const [selectedNode, setSelectedNode] = useState<GrafoNo | null>(null);

  if (grafo.nos.length === 0) {
    return (
      <EmptyState message="Nenhum relacionamento encontrado para este CNPJ." />
    );
  }

  function handleNodeClick(no: GrafoNo) {
    setSelectedNode((previous) => (previous?.id === no.id ? null : no));
  }

  return (
    <div className="flex gap-4">
      {/* Node grid */}
      <div className="flex-1 overflow-auto rounded-lg border border-gray-200 bg-gray-50 p-4">
        <div className="flex flex-wrap gap-3">
          {grafo.nos.map((no) => (
            <button
              key={no.id}
              onClick={() => handleNodeClick(no)}
              className={`flex max-w-[180px] flex-col rounded-lg border-2 px-3 py-2 text-left transition-shadow focus:outline-none focus:ring-2 focus:ring-blue-500 ${nodeColorClasses(no.tipo)} ${selectedNode?.id === no.id ? "shadow-lg ring-2 ring-offset-1" : "hover:shadow-md"}`}
            >
              <span className="line-clamp-2 text-xs font-medium leading-tight">
                {no.label}
              </span>
              {no.score !== null && (
                <span className="mt-1 text-xs opacity-70">
                  Score: {no.score}
                </span>
              )}
              {no.qtd_alertas !== null && no.qtd_alertas > 0 && (
                <span className="mt-0.5 text-xs font-semibold text-red-700">
                  {no.qtd_alertas} alerta{no.qtd_alertas > 1 ? "s" : ""}
                </span>
              )}
            </button>
          ))}
        </div>

        {/* Edge list */}
        {grafo.arestas.length > 0 && (
          <div className="mt-4 border-t border-gray-200 pt-4">
            <p className="mb-2 text-xs font-medium uppercase tracking-wide text-gray-400">
              Relacionamentos ({grafo.arestas.length})
            </p>
            <ul className="space-y-1">
              {grafo.arestas.map((aresta, index) => (
                <li
                  key={index}
                  className="flex items-center gap-2 text-xs text-gray-600"
                >
                  <span className="font-medium">{aresta.source}</span>
                  <span className="text-gray-400">→</span>
                  <span className="text-gray-400 italic">
                    {edgeLabel(aresta)}
                  </span>
                  <span className="text-gray-400">→</span>
                  <span className="font-medium">{aresta.target}</span>
                </li>
              ))}
            </ul>
          </div>
        )}
      </div>

      {/* Detail panel */}
      <div className="w-52 shrink-0 rounded-lg border border-gray-200 bg-white p-4">
        <p className="mb-3 text-xs font-semibold uppercase tracking-wide text-gray-400">
          Detalhes
        </p>
        <NoTooltip no={selectedNode} />
      </div>
    </div>
  );
}
