// SecaoDadosCadastrais — cadastral identity block for a supplier.
//
// Design decisions:
// - Renders a definition-list pattern (label + value rows) rather than a
//   generic table so screen readers can pair labels with values correctly.
// - SituacaoCadastral is color-coded: ATIVA green, everything else red/gray,
//   because active status is the normal expected state and deviations are
//   visually significant.
// - Endereço is optional per the FichaCompleta contract. When absent the
//   row is omitted entirely — no empty placeholder — to keep the form tight.

import { Card, CardHeader } from "@/components/ui/Card";
import { Badge } from "@/components/ui/Badge";
import { CNPJFormatado } from "@/components/CNPJFormatado";
import { ValorMonetario } from "@/components/ValorMonetario";
import { formatDate } from "@/lib/formatters";
import type { FichaCompleta, SituacaoCadastral } from "@/types/fornecedor";

const SITUACAO_CLASSES: Record<SituacaoCadastral, string> = {
  ATIVA: "bg-green-100 text-green-800",
  SUSPENSA: "bg-yellow-100 text-yellow-800",
  INAPTA: "bg-red-100 text-red-800",
  BAIXADA: "bg-gray-100 text-gray-700",
  NULA: "bg-gray-100 text-gray-700",
};

interface RowProps {
  label: string;
  children: React.ReactNode;
}

function Row({ label, children }: RowProps) {
  return (
    <div className="flex flex-col gap-0.5 sm:flex-row sm:gap-4">
      <dt className="w-40 shrink-0 text-xs font-medium text-gray-500">{label}</dt>
      <dd className="text-sm text-gray-900">{children}</dd>
    </div>
  );
}

interface SecaoDadosCadastraisProps {
  ficha: FichaCompleta;
}

export function SecaoDadosCadastrais({ ficha }: SecaoDadosCadastraisProps) {
  const {
    cnpj,
    razao_social,
    situacao,
    data_abertura,
    capital_social,
    cnae_principal,
    cnae_descricao,
    endereco,
  } = ficha;

  return (
    <Card>
      <CardHeader title="Dados Cadastrais" />
      <dl className="space-y-3">
        <Row label="CNPJ">
          <CNPJFormatado cnpj={cnpj} link={false} />
        </Row>

        <Row label="Razão Social">
          <span className="font-medium">{razao_social}</span>
        </Row>

        <Row label="Situação">
          <Badge className={SITUACAO_CLASSES[situacao]}>{situacao}</Badge>
        </Row>

        {data_abertura && (
          <Row label="Data de Abertura">
            {formatDate(data_abertura)}
          </Row>
        )}

        {capital_social && (
          <Row label="Capital Social">
            <ValorMonetario valor={capital_social} />
          </Row>
        )}

        {cnae_principal && (
          <Row label="CNAE Principal">
            <span className="font-mono text-xs">{cnae_principal}</span>
            {cnae_descricao && (
              <span className="ml-2 text-gray-600">{cnae_descricao}</span>
            )}
          </Row>
        )}

        {endereco && (
          <Row label="Endereço">
            {endereco.logradouro}, {endereco.municipio} — {endereco.uf},{" "}
            {endereco.cep}
          </Row>
        )}
      </dl>
    </Card>
  );
}
