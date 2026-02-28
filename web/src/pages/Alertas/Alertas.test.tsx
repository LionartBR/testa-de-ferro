// Tests for the Alertas page.
//
// Strategy:
// - Verify tipo and severidade filter controls are present in the DOM.
// - Verify changing the tipo filter triggers the server-side API call.
// - Verify severidade filtering is applied client-side on the already-fetched data.
// - Verify the Pagination component appears on successful loads.

// ---------------------------------------------------------------------------
// Module mocks — must appear before any imports that use these modules.
// ---------------------------------------------------------------------------

vi.mock("@/services/alertaService", () => ({
  getAlertas: vi.fn(),
  getAlertasPorTipo: vi.fn(),
}));

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import type { AlertaFeedItem } from "@/types/alerta";
import * as alertaService from "@/services/alertaService";
import { Alertas } from "./Alertas";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const ALERTA_GRAVISSIMO: AlertaFeedItem = {
  tipo: "SOCIO_SERVIDOR_PUBLICO",
  severidade: "GRAVISSIMO",
  descricao: "Sócio João Silva é servidor público federal",
  evidencia: "cpf_hmac=abc123",
  detectado_em: "2026-01-15T10:00:00",
  cnpj: "12345678000195",
  razao_social: "EMPRESA TESTE LTDA",
  socio_nome: "JOAO SILVA",
};

const ALERTA_GRAVE: AlertaFeedItem = {
  tipo: "RODIZIO_LICITACAO",
  severidade: "GRAVE",
  descricao: "Empresa participou de rodízio em licitações",
  evidencia: "licitacoes: [001/2024]",
  detectado_em: "2026-01-10T08:00:00",
  cnpj: "33000167000101",
  razao_social: "OUTRA EMPRESA SA",
  socio_nome: null,
};

const ALERTAS_FIXTURE: AlertaFeedItem[] = [ALERTA_GRAVISSIMO, ALERTA_GRAVE];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function renderAlertas() {
  return render(
    <MemoryRouter>
      <Alertas />
    </MemoryRouter>,
  );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("Alertas", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("renders alert feed with tipo and severidade filters", async () => {
    vi.mocked(alertaService.getAlertas).mockResolvedValue(ALERTAS_FIXTURE);

    renderAlertas();

    await screen.findByText("Sócio João Silva é servidor público federal");

    // Both filter controls must be present.
    expect(screen.getByLabelText("Tipo")).toBeTruthy();
    expect(screen.getByLabelText("Severidade")).toBeTruthy();
  });

  it("filters by tipo via API (server-side)", async () => {
    vi.mocked(alertaService.getAlertas).mockResolvedValue(ALERTAS_FIXTURE);
    vi.mocked(alertaService.getAlertasPorTipo).mockResolvedValue([
      ALERTA_GRAVISSIMO,
    ]);

    renderAlertas();

    await screen.findByText("Sócio João Silva é servidor público federal");

    const tipoSelect = screen.getByLabelText("Tipo");
    fireEvent.change(tipoSelect, { target: { value: "SOCIO_SERVIDOR_PUBLICO" } });

    expect(vi.mocked(alertaService.getAlertasPorTipo)).toHaveBeenCalledWith(
      "SOCIO_SERVIDOR_PUBLICO",
      expect.any(Number),
      expect.any(Number),
    );
  });

  it("filters by severidade client-side", async () => {
    vi.mocked(alertaService.getAlertas).mockResolvedValue(ALERTAS_FIXTURE);

    renderAlertas();

    await screen.findByText("Sócio João Silva é servidor público federal");
    await screen.findByText("Empresa participou de rodízio em licitações");

    const severidadeSelect = screen.getByLabelText("Severidade");
    fireEvent.change(severidadeSelect, { target: { value: "GRAVISSIMO" } });

    // Only the GRAVISSIMO item should remain.
    expect(
      screen.getByText("Sócio João Silva é servidor público federal"),
    ).toBeTruthy();
    expect(
      screen.queryByText("Empresa participou de rodízio em licitações"),
    ).toBeNull();
  });

  it("paginates through alerts", async () => {
    vi.mocked(alertaService.getAlertas).mockResolvedValue(ALERTAS_FIXTURE);

    renderAlertas();

    await screen.findByText("Sócio João Silva é servidor público federal");

    const prevButton = screen.getByRole("button", { name: "Anterior" });
    expect(prevButton).toBeTruthy();
  });
});
