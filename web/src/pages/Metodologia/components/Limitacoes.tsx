interface LimitacaoItem {
  titulo: string;
  descricao: string;
}

const LIMITACOES: LimitacaoItem[] = [
  {
    titulo: "Defasagem do QSA",
    descricao:
      "O Quadro Societário de Administradores (QSA) da Receita Federal reflete a situação no momento do processamento do arquivo mensal. Alterações societárias registradas nas Juntas Comerciais podem demorar semanas para aparecer na base.",
  },
  {
    titulo: "CPF mascarado dos servidores",
    descricao:
      "O Portal da Transparência exibe CPFs parcialmente mascarados (***XXX.XXX-**). O match com sócios é feito por nome completo + dígitos visíveis — há risco de falso negativo para homônimos com CPFs que diferem nos dígitos mascarados.",
  },
  {
    titulo: "Cobertura do mapeamento CNAE",
    descricao:
      "O indicador CNAE_INCOMPATIVEL é baseado em uma tabela de mapeamento manual que cobre os 50 CNAEs mais frequentes nas contratações públicas. CNAEs fora dessa lista não ativam o indicador, mesmo que incompatíveis.",
  },
  {
    titulo: "Endereço compartilhado em prédios comerciais",
    descricao:
      "O indicador MESMO_ENDERECO usa logradouro + número sem complemento para capturar fuzzy matches. Empresas legítimas em centros comerciais ou coworkings podem acionar o indicador indevidamente — por isso ele nunca gera alerta isolado, apenas contribui ao score.",
  },
  {
    titulo: "Abrangência do PNCP",
    descricao:
      "O PNCP tem cobertura obrigatória a partir de 2023. Contratos anteriores, especialmente de municípios e estados, podem não estar disponíveis, resultando em subestimação do volume histórico de alguns fornecedores.",
  },
  {
    titulo: "Doações eleitorais de pessoas físicas",
    descricao:
      "O indicador DOACAO_PARA_CONTRATANTE rastreia doações de sócios como pessoas físicas ao TSE. Doações realizadas via empresa (CNPJ) são proibidas desde 2015 e não aparecem na base. Sócios que doaram por meio de terceiros não são capturados.",
  },
  {
    titulo: "Dados não constituem prova jurídica",
    descricao:
      "Alertas e scores são indicadores de risco baseados em correlações estatísticas e cruzamento de bases públicas. Nenhum resultado desta plataforma constitui acusação formal, sentença ou prova jurídica de irregularidade.",
  },
];

export function Limitacoes() {
  return (
    <div className="space-y-4">
      {LIMITACOES.map((item) => (
        <div key={item.titulo} className="flex gap-3">
          <div className="mt-1 h-2 w-2 shrink-0 rounded-full bg-yellow-400" />
          <div>
            <p className="text-sm font-semibold text-gray-900">{item.titulo}</p>
            <p className="mt-1 text-sm text-gray-600">{item.descricao}</p>
          </div>
        </div>
      ))}
    </div>
  );
}
