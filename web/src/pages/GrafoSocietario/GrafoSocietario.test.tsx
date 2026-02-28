// Tests for the GrafoSocietario page.
//
// Strategy:
// - Verify the node/edge count summary text renders after a successful fetch.
// - Verify the truncation warning banner appears when grafo.truncado is true.
// - Verify the filter controls (Empresas / Sócios checkboxes) are present.
// - Verify node labels are visible in the canvas area.
//
// The GrafoCanvas is a div/button grid — no canvas element, no D3 — so all
// assertions work in jsdom without extra setup.

// ---------------------------------------------------------------------------
// Module mocks — must appear before any imports that use these modules.
// ---------------------------------------------------------------------------

vi.mock("@/services/fornecedorService", () => ({
  getFicha: vi.fn(),
  getRanking: vi.fn(),
  getGrafo: vi.fn(),
  exportar: vi.fn(),
}));

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import type { Grafo } from "@/types/grafo";
import * as fornecedorService from "@/services/fornecedorService";
import { GrafoSocietario } from "./GrafoSocietario";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const GRAFO_FIXTURE: Grafo = {
  nos: [
    {
      id: "12345678000195",
      tipo: "empresa",
      label: "EMPRESA TESTE",
      score: 45,
      qtd_alertas: 2,
    },
    {
      id: "socio_hmac_1",
      tipo: "socio",
      label: "JOAO SILVA",
      score: null,
      qtd_alertas: null,
    },
  ],
  arestas: [
    {
      source: "socio_hmac_1",
      target: "12345678000195",
      tipo: "socio",
      label: "Sócio-Administrador",
    },
  ],
  truncado: false,
};

const GRAFO_TRUNCADO_FIXTURE: Grafo = {
  ...GRAFO_FIXTURE,
  truncado: true,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function renderGrafo(cnpj = "12345678000195") {
  return render(
    <MemoryRouter initialEntries={[`/fornecedores/${cnpj}/grafo`]}>
      <Routes>
        <Route
          path="/fornecedores/:cnpj/grafo"
          element={<GrafoSocietario />}
        />
      </Routes>
    </MemoryRouter>,
  );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("GrafoSocietario", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("renders graph nodes and edges", async () => {
    vi.mocked(fornecedorService.getGrafo).mockResolvedValue(GRAFO_FIXTURE);

    renderGrafo();

    // CardHeader shows "2 nós · 1 relacionamentos"
    const summary = await screen.findByText(/2 nós/);
    expect(summary).toBeTruthy();

    // Node labels are rendered as buttons inside GrafoCanvas.
    expect(screen.getByText("EMPRESA TESTE")).toBeTruthy();
    expect(screen.getByText("JOAO SILVA")).toBeTruthy();
  });

  it("shows truncation warning when graph is truncated", async () => {
    vi.mocked(fornecedorService.getGrafo).mockResolvedValue(
      GRAFO_TRUNCADO_FIXTURE,
    );

    renderGrafo();

    const warning = await screen.findByText(/O grafo foi truncado para 50 nós/);
    expect(warning).toBeTruthy();
  });

  it("filters nodes by type via controls", async () => {
    vi.mocked(fornecedorService.getGrafo).mockResolvedValue(GRAFO_FIXTURE);

    renderGrafo();

    await screen.findByText("EMPRESA TESTE");

    // Both filter checkboxes must be present.
    const sociosCheckbox = screen.getByRole("checkbox", { name: "Sócios" });
    expect(sociosCheckbox).toBeTruthy();
    expect(screen.getByRole("checkbox", { name: "Empresas" })).toBeTruthy();

    // Unchecking Sócios hides JOAO SILVA from the canvas.
    fireEvent.click(sociosCheckbox);
    expect(screen.queryByText("JOAO SILVA")).toBeNull();

    // EMPRESA TESTE remains visible.
    expect(screen.getByText("EMPRESA TESTE")).toBeTruthy();
  });

  it("shows node tooltip on click", async () => {
    vi.mocked(fornecedorService.getGrafo).mockResolvedValue(GRAFO_FIXTURE);

    renderGrafo();

    await screen.findByText("EMPRESA TESTE");

    // Before clicking, the detail panel shows the default prompt.
    expect(screen.getByText("Clique em um nó para ver detalhes.")).toBeTruthy();

    // Click the empresa node button.
    const empresaButton = screen.getByRole("button", { name: /EMPRESA TESTE/ });
    fireEvent.click(empresaButton);

    // NoTooltip renders the score value in the detail panel.
    // The EMPRESA TESTE fixture has score=45 — once selected it appears in the panel.
    const scoreLabel = await screen.findByText("Score de risco");
    expect(scoreLabel).toBeTruthy();
  });
});
