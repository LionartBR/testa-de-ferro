// FichaFornecedor — complete dossier page for a single supplier.
//
// Layout strategy:
// - Two-column grid on lg+: left column (2/3) for primary analytical content
//   (alerts, score, contracts, sanctions), right column (1/3) for contextual
//   data (cadastral details, partners, donations, graph preview).
// - DisclaimerBanner spans full width at the top so it is never missed.
// - ExportButtons are placed at the top right of the header area so they
//   are immediately visible without scrolling.
//
// Data flow:
// - useFicha provides the FichaCompleta discriminated union.
// - GrafoMini receives the Grafo fetched separately so the main ficha
//   load is not blocked by the (potentially heavy) graph traversal.
//   We pass null when the graph hasn't loaded yet — GrafoMini handles that.
//
// ADR: cnpj from useParams is typed string | undefined. We render an error
// page when it is absent (malformed route) rather than crashing with a
// runtime error downstream in getFicha.

import { useParams } from "react-router-dom";
import { useCallback } from "react";
import { PageContainer } from "@/components/layout/PageContainer";
import { Loading } from "@/components/ui/Loading";
import { ErrorState } from "@/components/ui/ErrorState";
import { useApi } from "@/hooks/useApi";
import { getGrafo } from "@/services/fornecedorService";
import { useFicha } from "./hooks/useFicha";
import { DisclaimerBanner } from "./components/DisclaimerBanner";
import { SecaoDadosCadastrais } from "./components/SecaoDadosCadastrais";
import { SecaoAlertas } from "./components/SecaoAlertas";
import { SecaoScore } from "./components/SecaoScore";
import { SecaoContratos } from "./components/SecaoContratos";
import { SecaoSocios } from "./components/SecaoSocios";
import { SecaoSancoes } from "./components/SecaoSancoes";
import { SecaoDoacoes } from "./components/SecaoDoacoes";
import { GrafoMini } from "./components/GrafoMini";
import { NotaOficial } from "./components/NotaOficial";
import { ExportButtons } from "./components/ExportButtons";
import { formatCNPJ } from "@/lib/formatters";

function MissingCnpj() {
  return (
    <PageContainer>
      <ErrorState message="CNPJ não identificado na URL." />
    </PageContainer>
  );
}

interface FichaLoadedProps {
  cnpj: string;
}

function FichaLoaded({ cnpj }: FichaLoadedProps) {
  const fichaState = useFicha(cnpj);

  const grafoFetcher = useCallback(() => getGrafo(cnpj), [cnpj]);
  const grafoState = useApi(grafoFetcher);

  if (fichaState.status === "loading" || fichaState.status === "idle") {
    return (
      <PageContainer>
        <Loading />
      </PageContainer>
    );
  }

  if (fichaState.status === "error") {
    return (
      <PageContainer>
        <ErrorState
          message={fichaState.error.detail}
          onRetry={fichaState.refetch}
        />
      </PageContainer>
    );
  }

  const ficha = fichaState.data;
  const grafo = grafoState.status === "success" ? grafoState.data : null;

  return (
    <PageContainer>
      {/* Page heading */}
      <div className="mb-4 flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="text-lg font-bold text-gray-900">{ficha.razao_social}</h1>
          <p className="font-mono text-sm text-gray-500">{formatCNPJ(cnpj)}</p>
        </div>
        <ExportButtons cnpj={cnpj} />
      </div>

      {/* Disclaimer always visible */}
      <div className="mb-6">
        <DisclaimerBanner message={ficha.disclaimer} />
      </div>

      {/* Two-column layout */}
      <div className="grid grid-cols-1 gap-5 lg:grid-cols-3">
        {/* Left: primary analytical content — 2 columns */}
        <div className="space-y-5 lg:col-span-2">
          <SecaoAlertas alertas={ficha.alertas_criticos} />
          <SecaoScore score={ficha.score} />
          <SecaoContratos
            contratos={ficha.contratos}
            totalContratos={ficha.total_contratos}
            valorTotalContratos={ficha.valor_total_contratos}
          />
          <SecaoSancoes sancoes={ficha.sancoes} />
        </div>

        {/* Right: contextual data — 1 column */}
        <div className="space-y-5">
          <SecaoDadosCadastrais ficha={ficha} />
          <SecaoSocios socios={ficha.socios} />
          <SecaoDoacoes doacoes={ficha.doacoes} />
          <GrafoMini cnpj={cnpj} grafo={grafo} />
          <NotaOficial notaUrl={null} />
        </div>
      </div>
    </PageContainer>
  );
}

export function FichaFornecedor() {
  const { cnpj } = useParams<{ cnpj: string }>();

  if (!cnpj) {
    return <MissingCnpj />;
  }

  return <FichaLoaded cnpj={cnpj} />;
}
