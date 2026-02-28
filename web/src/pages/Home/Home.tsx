import { PageContainer } from "@/components/layout/PageContainer";
import { FreshnessBanner } from "@/components/FreshnessBanner";
import { Loading } from "@/components/ui/Loading";
import { ErrorState } from "@/components/ui/ErrorState";
import { Pagination } from "@/components/ui/Pagination";
import { AlertaFeed } from "./components/AlertaFeed";
import { ResumoGeral } from "./components/ResumoGeral";
import { useAlertasFeed } from "./hooks/useAlertasFeed";
import { useStats } from "./hooks/useStats";

export function Home() {
  const feed = useAlertasFeed();
  const stats = useStats();

  return (
    <PageContainer>
      <h1 className="text-xl font-bold text-gray-900">
        Testa de Ferro
      </h1>
      <p className="mt-1 text-sm text-gray-500">
        Monitoramento de fornecedores do governo federal com perfil suspeito.
      </p>

      {stats.status === "success" && (
        <div className="mt-6 space-y-4">
          <ResumoGeral stats={stats.data} />
          {Object.keys(stats.data.fontes).length > 0 && (
            <FreshnessBanner fontes={stats.data.fontes} />
          )}
        </div>
      )}

      <section className="mt-8">
        <h2 className="mb-4 text-lg font-semibold text-gray-900">
          Alertas Recentes
        </h2>

        {feed.status === "loading" && <Loading />}
        {feed.status === "error" && (
          <ErrorState message={feed.error.detail} onRetry={feed.refetch} />
        )}
        {feed.status === "success" && (
          <>
            <AlertaFeed alertas={feed.data} />
            <div className="mt-4">
              <Pagination
                page={feed.pagination.page}
                hasMore={feed.data.length === feed.pagination.limit}
                onPrev={feed.pagination.prevPage}
                onNext={feed.pagination.nextPage}
              />
            </div>
          </>
        )}
      </section>
    </PageContainer>
  );
}
