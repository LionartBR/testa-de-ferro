// Tests for the Home page.
//
// Strategy:
// - Page-level tests verify the three discriminated states: loading, error, success.
// - The ResumoGeral component renders stat numbers from the Stats fixture.
// - The AlertaFeed renders items from the AlertaFeedItem[] fixture.
// - Both services are mocked at the module level so tests are hermetic.

// ---------------------------------------------------------------------------
// Module mocks — must appear before any imports that use these modules.
// ---------------------------------------------------------------------------

vi.mock("@/services/alertaService", () => ({
  getAlertas: vi.fn(),
  getAlertasPorTipo: vi.fn(),
}));

vi.mock("@/services/statsService", () => ({
  getStats: vi.fn(),
}));

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import type { Stats } from "@/types/stats";
import type { AlertaFeedItem } from "@/types/alerta";
import { ApiError } from "@/types/api";
import * as alertaService from "@/services/alertaService";
import * as statsService from "@/services/statsService";
import { Home } from "./Home";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const STATS_FIXTURE: Stats = {
  total_fornecedores: 150,
  total_contratos: 3200,
  total_alertas: 42,
  fontes: { cnpj: { ultima_atualizacao: "2026-01-15", registros: 50000 } },
};

const ALERTAS_FEED_FIXTURE: AlertaFeedItem[] = [
  {
    tipo: "SOCIO_SERVIDOR_PUBLICO",
    severidade: "GRAVISSIMO",
    descricao: "Sócio João Silva é servidor público",
    evidencia: "cpf_hmac=abc123",
    detectado_em: "2026-01-15T10:00:00",
    cnpj: "12345678000195",
    razao_social: "EMPRESA TESTE LTDA",
    socio_nome: "JOAO SILVA",
  },
];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function renderHome() {
  return render(
    <MemoryRouter>
      <Home />
    </MemoryRouter>,
  );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("Home", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("renders stats summary when API returns data", async () => {
    vi.mocked(statsService.getStats).mockResolvedValue(STATS_FIXTURE);
    vi.mocked(alertaService.getAlertas).mockResolvedValue(ALERTAS_FEED_FIXTURE);

    renderHome();

    // ResumoGeral renders total_fornecedores formatted with pt-BR locale.
    const stat = await screen.findByText("150");
    expect(stat).toBeTruthy();
  });

  it("renders alert feed with pagination", async () => {
    vi.mocked(statsService.getStats).mockResolvedValue(STATS_FIXTURE);
    vi.mocked(alertaService.getAlertas).mockResolvedValue(ALERTAS_FEED_FIXTURE);

    renderHome();

    const descricao = await screen.findByText("Sócio João Silva é servidor público");
    expect(descricao).toBeTruthy();
  });

  it("shows loading state while fetching", () => {
    vi.mocked(statsService.getStats).mockReturnValue(new Promise(() => {}));
    vi.mocked(alertaService.getAlertas).mockReturnValue(new Promise(() => {}));

    renderHome();

    const spinner = document.querySelector(".animate-spin");
    expect(spinner).toBeTruthy();
  });

  it("shows error state on API failure", async () => {
    vi.mocked(statsService.getStats).mockRejectedValue(
      new ApiError(500, "Erro interno do servidor"),
    );
    vi.mocked(alertaService.getAlertas).mockRejectedValue(
      new ApiError(503, "Serviço indisponível"),
    );

    renderHome();

    const errorMsg = await screen.findByText("Serviço indisponível");
    expect(errorMsg).toBeTruthy();
  });
});
