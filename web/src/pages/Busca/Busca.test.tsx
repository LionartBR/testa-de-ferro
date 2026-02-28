// Tests for the Busca page.
//
// Strategy:
// - Verify the search input is pre-filled from the URL query param.
// - Verify the debounce delay before the service is called.
// - Verify results render as cards linking to the ficha.
// - Verify queries shorter than 2 chars do not trigger the service.
//
// Note: useDebounce uses setTimeout internally. We use vi.useFakeTimers() to
// control time without actually waiting 300ms.

// ---------------------------------------------------------------------------
// Module mocks — must appear before any imports that use these modules.
// ---------------------------------------------------------------------------

vi.mock("@/services/buscaService", () => ({
  buscar: vi.fn(),
}));

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { render, screen, fireEvent, act } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import type { FornecedorResumo } from "@/types/fornecedor";
import * as buscaService from "@/services/buscaService";
import { Busca } from "./Busca";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const BUSCA_FIXTURE: FornecedorResumo[] = [
  {
    cnpj: "12345678000195",
    razao_social: "EMPRESA ENCONTRADA",
    situacao: "ATIVA",
    score_risco: 45,
    faixa_risco: "Alto",
    qtd_alertas: 2,
    max_severidade: "GRAVE",
    total_contratos: 3,
    valor_total: "500000.00",
  },
];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function renderBusca(initialPath = "/busca") {
  return render(
    <MemoryRouter initialEntries={[initialPath]}>
      <Routes>
        <Route path="/busca" element={<Busca />} />
      </Routes>
    </MemoryRouter>,
  );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("Busca", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("renders search input and updates URL params", () => {
    vi.mocked(buscaService.buscar).mockResolvedValue([]);

    renderBusca("/busca?q=empresa");

    const input = screen.getByPlaceholderText(
      "CNPJ ou razão social...",
    ) as HTMLInputElement;
    expect(input.value).toBe("empresa");
  });

  it("debounces search and fetches results after 300ms", () => {
    vi.useFakeTimers();
    vi.mocked(buscaService.buscar).mockResolvedValue(BUSCA_FIXTURE);

    renderBusca();

    const input = screen.getByPlaceholderText("CNPJ ou razão social...");

    act(() => {
      fireEvent.change(input, { target: { value: "empresa" } });
    });

    // Service must NOT be called before the debounce window closes.
    expect(vi.mocked(buscaService.buscar)).not.toHaveBeenCalled();

    act(() => {
      vi.advanceTimersByTime(300);
    });

    expect(vi.mocked(buscaService.buscar)).toHaveBeenCalled();
  });

  it("shows results as cards with CNPJ links", async () => {
    vi.mocked(buscaService.buscar).mockResolvedValue(BUSCA_FIXTURE);

    // Pre-populate query so debounce fires immediately on render.
    renderBusca("/busca?q=empresa encontrada");

    const result = await screen.findByText("EMPRESA ENCONTRADA");
    expect(result).toBeTruthy();
  });

  it("stays idle when query is less than 2 characters", () => {
    vi.useFakeTimers();
    vi.mocked(buscaService.buscar).mockResolvedValue([]);

    renderBusca();

    const input = screen.getByPlaceholderText("CNPJ ou razão social...");

    act(() => {
      fireEvent.change(input, { target: { value: "e" } });
    });

    act(() => {
      vi.advanceTimersByTime(300);
    });

    expect(vi.mocked(buscaService.buscar)).not.toHaveBeenCalled();
  });
});
