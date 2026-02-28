// Tests for the DashboardOrgao page.
//
// Strategy:
// - Verify the orgao name and total_contratado appear in the summary section.
// - Verify the top fornecedores table renders the expected razao_social.
// - Verify loading spinner and error message appear in the expected states.

// ---------------------------------------------------------------------------
// Module mocks — must appear before any imports that use these modules.
// ---------------------------------------------------------------------------

vi.mock("@/services/orgaoService", () => ({
  getDashboard: vi.fn(),
}));

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import type { DashboardOrgao } from "@/types/orgao";
import { ApiError } from "@/types/api";
import * as orgaoService from "@/services/orgaoService";
import { DashboardOrgao as DashboardOrgaoPage } from "./DashboardOrgao";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const DASHBOARD_FIXTURE: DashboardOrgao = {
  orgao: { nome: "Ministério da Educação", sigla: "MEC", codigo: "26000" },
  qtd_contratos: 150,
  total_contratado: "25000000.00",
  qtd_fornecedores: 45,
  top_fornecedores: [
    {
      cnpj: "12345678000195",
      razao_social: "FORNECEDOR TOP",
      score_risco: 65,
      valor_total: "5000000.00",
      qtd_contratos: 12,
    },
  ],
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function renderDashboard(codigo = "26000") {
  return render(
    <MemoryRouter initialEntries={[`/orgaos/${codigo}/dashboard`]}>
      <Routes>
        <Route
          path="/orgaos/:codigo/dashboard"
          element={<DashboardOrgaoPage />}
        />
      </Routes>
    </MemoryRouter>,
  );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("DashboardOrgao", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("renders orgao summary with total contratado", async () => {
    vi.mocked(orgaoService.getDashboard).mockResolvedValue(DASHBOARD_FIXTURE);

    renderDashboard();

    // OrgaoResumo renders "MEC — Ministério da Educação"
    const orgaoName = await screen.findByText(
      "MEC — Ministério da Educação",
    );
    expect(orgaoName).toBeTruthy();

    // ValorMonetario renders "R$ 25.000.000,00"
    const valorElement = screen.getByText(/25\.000\.000/);
    expect(valorElement).toBeTruthy();
  });

  it("renders top fornecedores table", async () => {
    vi.mocked(orgaoService.getDashboard).mockResolvedValue(DASHBOARD_FIXTURE);

    renderDashboard();

    // The name appears both in the table and in the ContratosChart bar.
    const fornecedorNames = await screen.findAllByText("FORNECEDOR TOP");
    expect(fornecedorNames.length).toBeGreaterThanOrEqual(1);
  });

  it("shows loading and error states", async () => {
    vi.mocked(orgaoService.getDashboard).mockReturnValue(new Promise(() => {}));

    renderDashboard();

    const spinner = document.querySelector(".animate-spin");
    expect(spinner).toBeTruthy();

    // Re-render with error state.
    vi.mocked(orgaoService.getDashboard).mockRejectedValue(
      new ApiError(404, "Órgão não encontrado"),
    );

    render(
      <MemoryRouter initialEntries={["/orgaos/99999/dashboard"]}>
        <Routes>
          <Route
            path="/orgaos/:codigo/dashboard"
            element={<DashboardOrgaoPage />}
          />
        </Routes>
      </MemoryRouter>,
    );

    const errorMsg = await screen.findByText("Órgão não encontrado");
    expect(errorMsg).toBeTruthy();
  });
});
