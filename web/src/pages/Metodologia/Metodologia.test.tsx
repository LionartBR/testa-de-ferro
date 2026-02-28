// Tests for the Metodologia page.
//
// This is a fully static page — no API calls, no mocks needed.
// Tests verify that all four main section headings are present, confirming
// that each sub-component (ExplicacaoIndicadores, ExplicacaoAlertas,
// FontesDados, Limitacoes) is mounted and renders its section.

import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { Metodologia } from "./Metodologia";

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

function renderMetodologia() {
  return render(
    <MemoryRouter>
      <Metodologia />
    </MemoryRouter>,
  );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("Metodologia", () => {
  it("renders all indicator explanations", () => {
    renderMetodologia();

    // Section title from <Section title="Indicadores Cumulativos (Score)">.
    expect(
      screen.getByText("Indicadores Cumulativos (Score)"),
    ).toBeTruthy();
  });

  it("renders all alert explanations", () => {
    renderMetodologia();

    // Section title from <Section title="Alertas Críticos">.
    expect(screen.getByText("Alertas Críticos")).toBeTruthy();
  });

  it("renders data sources list", () => {
    renderMetodologia();

    // Section title from <Section title="Fontes de Dados">.
    expect(screen.getByText("Fontes de Dados")).toBeTruthy();
  });

  it("renders known limitations", () => {
    renderMetodologia();

    // Section title from <Section title="Limitações Conhecidas">.
    expect(screen.getByText("Limitações Conhecidas")).toBeTruthy();
  });
});
