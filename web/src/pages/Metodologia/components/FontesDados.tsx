interface FonteItem {
  nome: string;
  sigla: string;
  descricao: string;
  url: string;
  frequencia: string;
}

const FONTES: FonteItem[] = [
  {
    nome: "Receita Federal — Cadastro Nacional de Pessoas Jurídicas",
    sigla: "CNPJ",
    descricao:
      "Base completa de empresas brasileiras: razão social, CNAE, endereço, situação cadastral e quadro societário (QSA). Atualizada mensalmente pela Receita Federal.",
    url: "https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica-cnpj",
    frequencia: "Mensal",
  },
  {
    nome: "Portal Nacional de Contratações Públicas",
    sigla: "PNCP",
    descricao:
      "Contratos, licitações e atas de registro de preços do governo federal. Fonte primária para identificar quais empresas contratam com quais órgãos.",
    url: "https://pncp.gov.br",
    frequencia: "Diária",
  },
  {
    nome: "Cadastro de Empresas Inidôneas e Suspensas",
    sigla: "CEIS",
    descricao:
      "Lista de empresas e pessoas físicas suspensas de contratar com o poder público. Mantido pela CGU.",
    url: "https://dados.gov.br/dados/conjuntos-dados/ceis",
    frequencia: "Semanal",
  },
  {
    nome: "Cadastro Nacional de Empresas Punidas",
    sigla: "CNEP",
    descricao:
      "Sanções aplicadas com base na Lei Anticorrupção (Lei 12.846/2013). Inclui acordos de leniência. Mantido pela CGU.",
    url: "https://dados.gov.br/dados/conjuntos-dados/cnep",
    frequencia: "Semanal",
  },
  {
    nome: "Portal da Transparência — Servidores Federais",
    sigla: "Servidores",
    descricao:
      "Servidores públicos federais ativos, aposentados e pensionistas. CPF parcialmente mascarado. Usado para cruzamento com o QSA das empresas fornecedoras.",
    url: "https://portaldatransparencia.gov.br/download-de-dados/servidores",
    frequencia: "Mensal",
  },
  {
    nome: "Tribunal Superior Eleitoral — Prestação de Contas",
    sigla: "TSE",
    descricao:
      "Doações eleitorais declaradas por empresas e pessoas físicas a candidatos e partidos políticos. Cobre campanhas desde 2014.",
    url: "https://dadosabertos.tse.jus.br",
    frequencia: "Por ciclo eleitoral",
  },
];

export function FontesDados() {
  return (
    <div className="space-y-4">
      {FONTES.map((fonte) => (
        <div
          key={fonte.sigla}
          className="rounded-lg border border-gray-200 bg-white px-4 py-3"
        >
          <div className="flex items-start justify-between gap-4">
            <div className="flex-1">
              <div className="flex items-center gap-2">
                <span className="rounded-md bg-gray-100 px-2 py-0.5 font-mono text-xs font-semibold text-gray-700">
                  {fonte.sigla}
                </span>
                <span className="text-sm font-medium text-gray-900">
                  {fonte.nome}
                </span>
              </div>
              <p className="mt-1.5 text-sm text-gray-600">{fonte.descricao}</p>
            </div>
            <div className="shrink-0 text-right">
              <span className="block text-xs text-gray-400">Atualização</span>
              <span className="text-xs font-medium text-gray-700">
                {fonte.frequencia}
              </span>
            </div>
          </div>
          <a
            href={fonte.url}
            target="_blank"
            rel="noopener noreferrer"
            className="mt-2 block text-xs text-blue-600 hover:underline"
          >
            {fonte.url}
          </a>
        </div>
      ))}
    </div>
  );
}
