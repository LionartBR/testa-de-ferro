import { useState } from "react";
import { useParams } from "react-router-dom";
import { PageContainer } from "@/components/layout/PageContainer";
import { Loading } from "@/components/ui/Loading";
import { ErrorState } from "@/components/ui/ErrorState";
import { Card, CardHeader } from "@/components/ui/Card";
import { useGrafo } from "./hooks/useGrafo";
import { useGrafoExpansion } from "./hooks/useGrafoExpansion";
import { GrafoCanvas } from "./components/GrafoCanvas";
import { GrafoControles } from "./components/GrafoControles";
import { GrafoLegenda } from "./components/GrafoLegenda";
import type { GrafoDisplayFilters } from "./types";
import type { Grafo } from "@/types/grafo";

function filterGrafo(grafo: Grafo, filters: GrafoDisplayFilters): Grafo {
  const visibleIds = new Set(
    grafo.nos
      .filter(
        (no) =>
          (no.tipo === "empresa" && filters.showEmpresas) ||
          (no.tipo === "socio" && filters.showSocios),
      )
      .map((no) => no.id),
  );

  return {
    nos: grafo.nos.filter((no) => visibleIds.has(no.id)),
    arestas: grafo.arestas.filter(
      (aresta) =>
        visibleIds.has(aresta.source) && visibleIds.has(aresta.target),
    ),
    truncado: grafo.truncado,
  };
}

export function GrafoSocietario() {
  const { cnpj } = useParams<{ cnpj: string }>();
  const { status, data: grafo, error, refetch } = useGrafo();
  const { resetExpansion } = useGrafoExpansion();

  const [filters, setFilters] = useState<GrafoDisplayFilters>({
    showEmpresas: true,
    showSocios: true,
  });

  function handleToggleEmpresas() {
    setFilters((previous) => ({
      ...previous,
      showEmpresas: !previous.showEmpresas,
    }));
  }

  function handleToggleSocios() {
    setFilters((previous) => ({
      ...previous,
      showSocios: !previous.showSocios,
    }));
  }

  function handleReset() {
    setFilters({ showEmpresas: true, showSocios: true });
    resetExpansion();
  }

  const subtitle = cnpj ? `CNPJ ${cnpj}` : undefined;

  return (
    <PageContainer>
      <div className="space-y-4">
        <div>
          <h1 className="text-xl font-bold text-gray-900">
            Grafo Societário
          </h1>
          {subtitle && (
            <p className="mt-1 font-mono text-sm text-gray-500">{subtitle}</p>
          )}
        </div>

        {status === "loading" && <Loading />}

        {status === "error" && (
          <ErrorState
            message={error.detail}
            onRetry={refetch}
          />
        )}

        {status === "success" && (
          <div className="space-y-3">
            {grafo.truncado && (
              <div className="rounded-md border border-yellow-200 bg-yellow-50 px-4 py-3">
                <p className="text-sm text-yellow-800">
                  O grafo foi truncado para 50 nós. Relacionamentos indiretos
                  podem não estar visíveis.
                </p>
              </div>
            )}

            <Card>
              <CardHeader
                title={`${grafo.nos.length} nós · ${grafo.arestas.length} relacionamentos`}
                action={<GrafoLegenda />}
              />
              <div className="mb-4">
                <GrafoControles
                  showEmpresas={filters.showEmpresas}
                  showSocios={filters.showSocios}
                  onToggleEmpresas={handleToggleEmpresas}
                  onToggleSocios={handleToggleSocios}
                  onReset={handleReset}
                />
              </div>
              <GrafoCanvas grafo={filterGrafo(grafo, filters)} />
            </Card>
          </div>
        )}
      </div>
    </PageContainer>
  );
}
