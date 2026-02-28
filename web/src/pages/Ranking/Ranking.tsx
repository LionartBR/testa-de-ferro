import { useState } from "react";
import { PageContainer } from "@/components/layout/PageContainer";
import { Loading } from "@/components/ui/Loading";
import { ErrorState } from "@/components/ui/ErrorState";
import { EmptyState } from "@/components/ui/EmptyState";
import { Pagination } from "@/components/ui/Pagination";
import { useRanking } from "./hooks/useRanking";
import { RankingTabela } from "./components/RankingTabela";
import { RankingFiltros } from "./components/RankingFiltros";
import type { FaixaRisco } from "@/types/score";
import type { FornecedorResumo } from "@/types/fornecedor";

export function Ranking() {
  const [faixaFiltro, setFaixaFiltro] = useState<FaixaRisco | "">("");
  const { status, data, error, refetch, page, limit, offset, nextPage, prevPage } =
    useRanking();

  const filtered: FornecedorResumo[] =
    status === "success" && data
      ? faixaFiltro
        ? data.filter((item) => item.faixa_risco === faixaFiltro)
        : data
      : [];

  const hasMore = status === "success" && data != null && data.length === limit;

  return (
    <PageContainer>
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-xl font-semibold text-gray-900">
              Ranking de Risco
            </h1>
            <p className="mt-1 text-sm text-gray-500">
              Fornecedores ordenados por score cumulativo de risco
            </p>
          </div>
          <RankingFiltros
            faixaAtual={faixaFiltro}
            onFaixaChange={setFaixaFiltro}
          />
        </div>

        {status === "loading" && <Loading />}

        {status === "error" && (
          <ErrorState
            message={error.detail}
            onRetry={refetch}
          />
        )}

        {status === "success" && filtered.length === 0 && (
          <EmptyState message="Nenhum fornecedor encontrado para essa faixa de risco." />
        )}

        {status === "success" && filtered.length > 0 && (
          <>
            <RankingTabela data={filtered} offset={faixaFiltro ? 0 : offset} />
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
