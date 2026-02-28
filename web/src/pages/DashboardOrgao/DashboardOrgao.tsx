import { PageContainer } from "@/components/layout/PageContainer";
import { Loading } from "@/components/ui/Loading";
import { ErrorState } from "@/components/ui/ErrorState";
import { useDashboardOrgao } from "./hooks/useDashboardOrgao";
import { OrgaoResumo } from "./components/OrgaoResumo";
import { TopFornecedores } from "./components/TopFornecedores";
import { ContratosChart } from "./components/ContratosChart";

export function DashboardOrgao() {
  const { status, data: dashboard, error, refetch } = useDashboardOrgao();

  return (
    <PageContainer>
      <div className="space-y-6">
        <h1 className="text-xl font-bold text-gray-900">Dashboard do Órgão</h1>

        {status === "loading" && <Loading />}

        {status === "error" && (
          <ErrorState message={error.detail} onRetry={refetch} />
        )}

        {status === "success" && (
          <div className="space-y-6">
            <OrgaoResumo dashboard={dashboard} />
            <TopFornecedores fornecedores={dashboard.top_fornecedores} />
            <ContratosChart fornecedores={dashboard.top_fornecedores} />
          </div>
        )}
      </div>
    </PageContainer>
  );
}
