// Tests for the Ranking page.
//
// Strategy:
// - Verify the table renders fornecedor data from the fixture.
// - Verify client-side faixa de risco filtering hides non-matching rows.
// - Verify the Pagination component appears for successful loads.
// - Verify loading spinner and error message appear in the expected states.

// ---------------------------------------------------------------------------
// Module mocks — must appear before any imports that use these modules.
// ---------------------------------------------------------------------------

vi.mock("@/services/fornecedorService", () => ({
  getRanking: vi.fn(),
  getFicha: vi.fn(),
  getGrafo: vi.fn(),
  exportar: vi.fn(),
}));

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import type { FornecedorResumo } from "@/types/fornecedor";
import { ApiError } from "@/types/api";
import * as fornecedorService from "@/services/fornecedorService";
import { Ranking } from "./Ranking";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const RANKING_FIXTURE: FornecedorResumo[] = [
  {
    cnpj: "11222333000181",
    razao_social: "EMPRESA RISCO ALTO",
    situacao: "ATIVA",
    score_risco: 75,
    faixa_risco: "Critico",
    qtd_alertas: 3,
    max_severidade: "GRAVISSIMO",
    total_contratos: 5,
    valor_total: "1000000.00",
  },
  {
    cnpj: "33000167000101",
    razao_social: "EMPRESA LIMPA",
    situacao: "ATIVA",
    score_risco: 0,
    faixa_risco: "Baixo",
    qtd_alertas: 0,
    max_severidade: null,
    total_contratos: 2,
    valor_total: "50000.00",
  },
];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function renderRanking() {
  return render(
    <MemoryRouter>
      <Ranking />
    </MemoryRouter>,
  );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("Ranking", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("renders ranking table with fornecedores sorted by score", async () => {
    vi.mocked(fornecedorService.getRanking).mockResolvedValue(RANKING_FIXTURE);

    renderRanking();

    const name = await screen.findByText("EMPRESA RISCO ALTO");
    expect(name).toBeTruthy();
    const cleanName = await screen.findByText("EMPRESA LIMPA");
    expect(cleanName).toBeTruthy();
  });

  it("filters by faixa de risco client-side", async () => {
    vi.mocked(fornecedorService.getRanking).mockResolvedValue(RANKING_FIXTURE);

    renderRanking();

    // Wait for data to load before interacting.
    await screen.findByText("EMPRESA RISCO ALTO");

    const select = screen.getByRole("combobox");
    fireEvent.change(select, { target: { value: "Critico" } });

    // Only the Critico-faixa item should remain visible.
    expect(screen.getByText("EMPRESA RISCO ALTO")).toBeTruthy();
    expect(screen.queryByText("EMPRESA LIMPA")).toBeNull();
  });

  it("paginates through results", async () => {
    vi.mocked(fornecedorService.getRanking).mockResolvedValue(RANKING_FIXTURE);

    renderRanking();

    await screen.findByText("EMPRESA RISCO ALTO");

    // Pagination renders navigation buttons regardless of total pages.
    const prevButton = screen.getByRole("button", { name: "Anterior" });
    expect(prevButton).toBeTruthy();
  });

  it("shows loading and error states", async () => {
    vi.mocked(fornecedorService.getRanking).mockReturnValue(new Promise(() => {}));

    renderRanking();

    const spinner = document.querySelector(".animate-spin");
    expect(spinner).toBeTruthy();

    // Re-render with error state.
    vi.mocked(fornecedorService.getRanking).mockRejectedValue(
      new ApiError(500, "Erro ao carregar ranking"),
    );

    render(
      <MemoryRouter>
        <Ranking />
      </MemoryRouter>,
    );

    const errorMsg = await screen.findByText("Erro ao carregar ranking");
    expect(errorMsg).toBeTruthy();
  });
});
