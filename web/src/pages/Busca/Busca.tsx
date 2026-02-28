import { useState, useEffect } from "react";
import { useSearchParams } from "react-router-dom";
import { PageContainer } from "@/components/layout/PageContainer";
import { Loading } from "@/components/ui/Loading";
import { ErrorState } from "@/components/ui/ErrorState";
import { EmptyState } from "@/components/ui/EmptyState";
import { useBusca } from "./hooks/useBusca";
import { BuscaInput } from "./components/BuscaInput";
import { ResultadoLista } from "./components/ResultadoLista";

export function Busca() {
  const [searchParams, setSearchParams] = useSearchParams();
  const initialQuery = searchParams.get("q") ?? "";
  const [query, setQuery] = useState(initialQuery);

  const { status, data, error, refetch } = useBusca(query);

  // Keep URL in sync with query so the page is bookmarkable.
  useEffect(() => {
    if (query) {
      setSearchParams({ q: query }, { replace: true });
    } else {
      setSearchParams({}, { replace: true });
    }
  }, [query, setSearchParams]);

  const showIdle = status === "idle";
  const showResults = status === "success" && data != null && data.length > 0;
  const showEmpty = status === "success" && (data == null || data.length === 0);

  return (
    <PageContainer>
      <div className="space-y-6">
        <div>
          <h1 className="text-xl font-semibold text-gray-900">Busca</h1>
          <p className="mt-1 text-sm text-gray-500">
            Pesquise fornecedores por CNPJ ou razão social
          </p>
        </div>

        <BuscaInput value={query} onChange={setQuery} />

        {showIdle && (
          <p className="text-center text-sm text-gray-400">
            Digite ao menos 2 caracteres para iniciar a busca.
          </p>
        )}

        {status === "loading" && <Loading />}

        {status === "error" && (
          <ErrorState message={error.detail} onRetry={refetch} />
        )}

        {showEmpty && (
          <EmptyState message="Nenhum fornecedor encontrado." />
        )}

        {showResults && <ResultadoLista resultados={data} />}
      </div>
    </PageContainer>
  );
}
