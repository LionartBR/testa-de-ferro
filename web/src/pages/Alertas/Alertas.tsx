import { useState } from "react";
import { PageContainer } from "@/components/layout/PageContainer";
import { Loading } from "@/components/ui/Loading";
import { ErrorState } from "@/components/ui/ErrorState";
import { EmptyState } from "@/components/ui/EmptyState";
import { Pagination } from "@/components/ui/Pagination";
import { useAlertas } from "./hooks/useAlertas";
import { AlertaLista } from "./components/AlertaLista";
import { AlertaFiltros } from "./components/AlertaFiltros";
import type { TipoAlerta, Severidade, AlertaFeedItem } from "@/types/alerta";

export function Alertas() {
  const [tipoFiltro, setTipoFiltro] = useState<TipoAlerta | null>(null);
  const [severidadeFiltro, setSeveridadeFiltro] = useState<Severidade | null>(null);

  const { status, data, error, refetch, page, limit, nextPage, prevPage } =
    useAlertas(tipoFiltro);

  // Severidade filtering is client-side since the API filters by tipo only.
  const filtered: AlertaFeedItem[] =
    status === "success" && data != null
      ? severidadeFiltro
        ? data.filter((alerta) => alerta.severidade === severidadeFiltro)
        : data
      : [];

  const hasMore = status === "success" && data != null && data.length === limit;

  return (
    <PageContainer>
      <div className="space-y-6">
        <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
          <div>
            <h1 className="text-xl font-semibold text-gray-900">Alertas</h1>
            <p className="mt-1 text-sm text-gray-500">
              Flags críticas detectadas nos fornecedores do governo federal
            </p>
          </div>
          <AlertaFiltros
            tipoAtual={tipoFiltro}
            severidadeAtual={severidadeFiltro}
            onTipoChange={setTipoFiltro}
            onSeveridadeChange={setSeveridadeFiltro}
          />
        </div>

        {status === "loading" && <Loading />}

        {status === "error" && (
          <ErrorState message={error.detail} onRetry={refetch} />
        )}

        {status === "success" && filtered.length === 0 && (
          <EmptyState message="Nenhum alerta encontrado para os filtros selecionados." />
        )}

        {status === "success" && filtered.length > 0 && (
          <>
            <AlertaLista alertas={filtered} />
            <Pagination
              page={page}
              hasMore={hasMore}
              onPrev={prevPage}
              onNext={nextPage}
            />
          </>
        )}
      </div>
    </PageContainer>
  );
}
