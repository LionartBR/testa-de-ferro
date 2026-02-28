// Tests for the FichaFornecedor page and its core subcomponents.
//
// Strategy:
// - Page-level tests focus on the discriminated states: loading, error,
//   and success rendering with real fixture data.
// - Subcomponent tests exercise the non-trivial logic: alert grouping,
//   situacao color coding, and the "vigente" highlight on sanctions.
// - We do NOT test styling classes — those are visual and belong in visual
//   regression tests. We test content, structure, and user-visible behavior.
// - API calls are mocked at the service module level so tests are hermetic.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import type { FichaCompleta } from "@/types/fornecedor";
import type { Score } from "@/types/score";
import type { AlertaCritico } from "@/types/alerta";
import type { Sancao } from "@/types/sancao";
import type { Grafo } from "@/types/grafo";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const SCORE_FIXTURE: Score = {
  valor: 45,
  faixa: "Moderado",
  indicadores: [
    {
      tipo: "EMPRESA_RECENTE",
      peso: 10,
      descricao: "Empresa com menos de 2 anos de operação",
      evidencia: "data_abertura: 2023-06-01",
    },
    {
      tipo: "CAPITAL_SOCIAL_BAIXO",
      peso: 15,
      descricao: "Capital social abaixo do esperado para o setor",
      evidencia: "capital: R$ 1.000,00",
    },
  ],
};

const ALERTAS_FIXTURE: AlertaCritico[] = [
  {
    tipo: "SOCIO_SERVIDOR_PUBLICO",
    severidade: "GRAVISSIMO",
    descricao: "Sócio João Silva é servidor do Ministério da Fazenda",
    evidencia: "cpf_hmac: abc123 | orgao: MIN_FAZENDA",
  },
  {
    tipo: "SOCIO_SERVIDOR_PUBLICO",
    severidade: "GRAVE",
    descricao: "Sócia Maria Souza é servidora do TCU",
    evidencia: "cpf_hmac: def456 | orgao: TCU",
  },
  {
    tipo: "RODIZIO_LICITACAO",
    severidade: "GRAVE",
    descricao: "Empresa participou de rodízio em 3 licitações",
    evidencia: "licitacoes: [001/2022, 002/2022, 003/2023]",
  },
];

const SANCOES_FIXTURE: Sancao[] = [
  {
    tipo: "CEIS",
    orgao_sancionador: "CGU",
    motivo: "Fraude em licitação pública",
    data_inicio: "2023-01-01",
    data_fim: null,
    vigente: true,
  },
  {
    tipo: "CNEP",
    orgao_sancionador: "Ministério da Justiça",
    motivo: "Descumprimento contratual",
    data_inicio: "2020-03-15",
    data_fim: "2022-03-14",
    vigente: false,
  },
];

const FICHA_FIXTURE: FichaCompleta = {
  cnpj: "12345678000195",
  razao_social: "EMPRESA TESTE LTDA",
  situacao: "ATIVA",
  data_abertura: "2023-06-01",
  capital_social: "1000.00",
  cnae_principal: "6201-5",
  cnae_descricao: "Desenvolvimento de programas de computador sob encomenda",
  endereco: {
    logradouro: "Rua das Flores, 100",
    municipio: "São Paulo",
    uf: "SP",
    cep: "01310-100",
  },
  total_contratos: 5,
  valor_total_contratos: "500000.00",
  alertas_criticos: ALERTAS_FIXTURE,
  score: SCORE_FIXTURE,
  socios: [
    {
      nome: "JOAO SILVA",
      qualificacao: "Sócio-Administrador",
      is_servidor_publico: true,
      orgao_lotacao: "Ministério da Fazenda",
    },
    {
      nome: "MARIA SOUZA",
      qualificacao: "Sócia",
      is_servidor_publico: false,
      orgao_lotacao: null,
    },
  ],
  sancoes: SANCOES_FIXTURE,
  contratos: [
    {
      orgao_codigo: "26000",
      valor: "250000.00",
      data_assinatura: "2024-01-15",
      objeto: "Fornecimento de software de gestão",
    },
  ],
  doacoes: [
    {
      candidato_nome: "CANDIDATO TESTE",
      candidato_partido: "PT",
      candidato_cargo: "Deputado Federal",
      valor: "15000.00",
      ano_eleicao: 2022,
      via_socio: true,
    },
  ],
  disclaimer:
    "Os dados apresentados são automatizados e não constituem acusação formal.",
};

const GRAFO_FIXTURE: Grafo = {
  nos: [
    { id: "12345678000195", tipo: "empresa", label: "EMPRESA TESTE LTDA", score: 45, qtd_alertas: 3 },
    { id: "JOAO_SILVA_hmac", tipo: "socio", label: "JOAO SILVA", score: null, qtd_alertas: null },
  ],
  arestas: [
    { source: "JOAO_SILVA_hmac", target: "12345678000195", tipo: "socio", label: "Sócio-Administrador" },
  ],
  truncado: false,
};

// ---------------------------------------------------------------------------
// Module mocks
// ---------------------------------------------------------------------------

vi.mock("@/services/fornecedorService", () => ({
  getFicha: vi.fn(),
  getGrafo: vi.fn(),
  exportar: vi.fn(),
}));

// Import after mock so the module uses the mocked version.
import * as fornecedorService from "@/services/fornecedorService";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function renderFicha(cnpj = "12345678000195") {
  return render(
    <MemoryRouter initialEntries={[`/fornecedores/${cnpj}`]}>
      <Routes>
        <Route
          path="/fornecedores/:cnpj"
          element={
            // Lazy import to avoid circular deps at mock time
            <FichaFornecedorWrapper />
          }
        />
      </Routes>
    </MemoryRouter>,
  );
}

// Wrapper that imports the component after mocks are set up.
import { FichaFornecedor } from "./FichaFornecedor";
function FichaFornecedorWrapper() {
  return <FichaFornecedor />;
}

// ---------------------------------------------------------------------------
// Subcomponent tests — pure logic, no routing
// ---------------------------------------------------------------------------

import { SecaoAlertas } from "./components/SecaoAlertas";
import { SecaoSancoes } from "./components/SecaoSancoes";
import { SecaoScore } from "./components/SecaoScore";
import { SecaoDadosCadastrais } from "./components/SecaoDadosCadastrais";
import { DisclaimerBanner } from "./components/DisclaimerBanner";

describe("SecaoAlertas", () => {
  it("groups alerts of the same type under a single expandable heading", () => {
    render(
      <MemoryRouter>
        <SecaoAlertas alertas={ALERTAS_FIXTURE} />
      </MemoryRouter>,
    );

    // Two unique tipos → two groups
    const summaries = screen.getAllByRole("group");
    expect(summaries.length).toBeGreaterThanOrEqual(2);
  });

  it("shows the total alert count badge", () => {
    render(
      <MemoryRouter>
        <SecaoAlertas alertas={ALERTAS_FIXTURE} />
      </MemoryRouter>,
    );

    expect(screen.getByText("3")).toBeTruthy();
  });

  it("shows empty state when there are no alerts", () => {
    render(
      <MemoryRouter>
        <SecaoAlertas alertas={[]} />
      </MemoryRouter>,
    );

    expect(
      screen.getByText("Nenhum alerta crítico identificado."),
    ).toBeTruthy();
  });
});

describe("SecaoSancoes", () => {
  it("renders all sanctions", () => {
    render(
      <MemoryRouter>
        <SecaoSancoes sancoes={SANCOES_FIXTURE} />
      </MemoryRouter>,
    );

    expect(screen.getByText("Fraude em licitação pública")).toBeTruthy();
    expect(screen.getByText("Descumprimento contratual")).toBeTruthy();
  });

  it("shows Vigente badge for active sanctions", () => {
    render(
      <MemoryRouter>
        <SecaoSancoes sancoes={SANCOES_FIXTURE} />
      </MemoryRouter>,
    );

    expect(screen.getByText("Vigente")).toBeTruthy();
  });

  it("shows Expirada badge for expired sanctions", () => {
    render(
      <MemoryRouter>
        <SecaoSancoes sancoes={SANCOES_FIXTURE} />
      </MemoryRouter>,
    );

    expect(screen.getByText("Expirada")).toBeTruthy();
  });

  it("shows Indefinida when data_fim is null", () => {
    render(
      <MemoryRouter>
        <SecaoSancoes sancoes={SANCOES_FIXTURE} />
      </MemoryRouter>,
    );

    expect(screen.getByText("Fim: Indefinida")).toBeTruthy();
  });

  it("shows empty state when no sanctions exist", () => {
    render(
      <MemoryRouter>
        <SecaoSancoes sancoes={[]} />
      </MemoryRouter>,
    );

    expect(screen.getByText("Nenhuma sanção registrada.")).toBeTruthy();
  });
});

describe("SecaoScore", () => {
  it("renders each indicator label and weight", () => {
    render(
      <MemoryRouter>
        <SecaoScore score={SCORE_FIXTURE} />
      </MemoryRouter>,
    );

    expect(screen.getByText("Empresa Recente")).toBeTruthy();
    expect(screen.getByText("Capital Social Baixo")).toBeTruthy();
    expect(screen.getByText("+10")).toBeTruthy();
    expect(screen.getByText("+15")).toBeTruthy();
  });

  it("shows empty state when score is null", () => {
    render(
      <MemoryRouter>
        <SecaoScore score={null} />
      </MemoryRouter>,
    );

    expect(
      screen.getByText("Nenhum indicador cumulativo identificado."),
    ).toBeTruthy();
  });

  it("shows empty state when score has no indicators", () => {
    const emptyScore: Score = { valor: 0, faixa: "Baixo", indicadores: [] };
    render(
      <MemoryRouter>
        <SecaoScore score={emptyScore} />
      </MemoryRouter>,
    );

    expect(
      screen.getByText("Nenhum indicador cumulativo identificado."),
    ).toBeTruthy();
  });
});

describe("SecaoDadosCadastrais", () => {
  it("renders razao_social and cnpj", () => {
    render(
      <MemoryRouter>
        <SecaoDadosCadastrais ficha={FICHA_FIXTURE} />
      </MemoryRouter>,
    );

    expect(screen.getByText("EMPRESA TESTE LTDA")).toBeTruthy();
    // CNPJ formatted: 12.345.678/0001-95
    expect(screen.getByText("12.345.678/0001-95")).toBeTruthy();
  });

  it("renders situacao badge", () => {
    render(
      <MemoryRouter>
        <SecaoDadosCadastrais ficha={FICHA_FIXTURE} />
      </MemoryRouter>,
    );

    expect(screen.getByText("ATIVA")).toBeTruthy();
  });

  it("renders formatted opening date", () => {
    render(
      <MemoryRouter>
        <SecaoDadosCadastrais ficha={FICHA_FIXTURE} />
      </MemoryRouter>,
    );

    expect(screen.getByText("01/06/2023")).toBeTruthy();
  });

  it("omits endereco section when endereco is null", () => {
    const fichaWithoutEndereco: FichaCompleta = {
      ...FICHA_FIXTURE,
      endereco: null,
    };

    render(
      <MemoryRouter>
        <SecaoDadosCadastrais ficha={fichaWithoutEndereco} />
      </MemoryRouter>,
    );

    expect(screen.queryByText("Endereço")).toBeNull();
  });
});

describe("DisclaimerBanner", () => {
  it("renders the disclaimer message with Aviso prefix", () => {
    render(
      <DisclaimerBanner message="Dados automáticos, não constituem acusação." />,
    );

    expect(screen.getByText("Aviso:")).toBeTruthy();
    expect(
      screen.getByText("Dados automáticos, não constituem acusação."),
    ).toBeTruthy();
  });

  it("uses note role for accessibility", () => {
    render(
      <DisclaimerBanner message="Aviso de teste." />,
    );

    expect(screen.getByRole("note")).toBeTruthy();
  });
});

// ---------------------------------------------------------------------------
// Page-level tests
// ---------------------------------------------------------------------------

describe("FichaFornecedor page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("shows loading state while fetching", () => {
    vi.mocked(fornecedorService.getFicha).mockReturnValue(new Promise(() => {}));
    vi.mocked(fornecedorService.getGrafo).mockReturnValue(new Promise(() => {}));

    renderFicha();

    // The spinner is present in the DOM during loading.
    const spinner = document.querySelector(".animate-spin");
    expect(spinner).toBeTruthy();
  });

  it("shows error state when API call fails", async () => {
    const { ApiError } = await import("@/types/api");
    vi.mocked(fornecedorService.getFicha).mockRejectedValue(
      new ApiError(404, "Fornecedor não encontrado"),
    );
    vi.mocked(fornecedorService.getGrafo).mockRejectedValue(
      new ApiError(404, "Não encontrado"),
    );

    renderFicha();

    const errorMsg = await screen.findByText("Fornecedor não encontrado");
    expect(errorMsg).toBeTruthy();
  });

  it("renders razao_social in the h1 heading after successful fetch", async () => {
    vi.mocked(fornecedorService.getFicha).mockResolvedValue(FICHA_FIXTURE);
    vi.mocked(fornecedorService.getGrafo).mockResolvedValue(GRAFO_FIXTURE);

    renderFicha();

    // findAllByText because the name also appears in SecaoDadosCadastrais.
    // We verify the heading element specifically.
    const heading = await screen.findByRole("heading", { level: 1 });
    expect(heading.textContent).toBe("EMPRESA TESTE LTDA");
  });

  it("renders disclaimer banner after successful fetch", async () => {
    vi.mocked(fornecedorService.getFicha).mockResolvedValue(FICHA_FIXTURE);
    vi.mocked(fornecedorService.getGrafo).mockResolvedValue(GRAFO_FIXTURE);

    renderFicha();

    const disclaimer = await screen.findByRole("note");
    expect(disclaimer).toBeTruthy();
    // Verify the backend-provided disclaimer text is rendered verbatim.
    expect(disclaimer.textContent).toContain("automatizados");
  });

  it("renders alert section with correct count after successful fetch", async () => {
    vi.mocked(fornecedorService.getFicha).mockResolvedValue(FICHA_FIXTURE);
    vi.mocked(fornecedorService.getGrafo).mockResolvedValue(GRAFO_FIXTURE);

    renderFicha();

    // 3 alertas in fixture → badge shows "3"
    const badge = await screen.findByText("3");
    expect(badge).toBeTruthy();
  });

  it("shows error when cnpj is missing from the route", () => {
    render(
      <MemoryRouter initialEntries={["/fornecedores/"]}>
        <Routes>
          <Route path="/fornecedores/" element={<FichaFornecedor />} />
        </Routes>
      </MemoryRouter>,
    );

    expect(screen.getByText("CNPJ não identificado na URL.")).toBeTruthy();
  });
});
