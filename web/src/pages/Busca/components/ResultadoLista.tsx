import { Link } from "react-router-dom";
import { Card } from "@/components/ui/Card";
import { ScoreBadge } from "@/components/ScoreBadge";
import { CNPJFormatado } from "@/components/CNPJFormatado";
import { ValorMonetario } from "@/components/ValorMonetario";
import type { FornecedorResumo } from "@/types/fornecedor";

interface ResultadoListaProps {
  resultados: FornecedorResumo[];
}

export function ResultadoLista({ resultados }: ResultadoListaProps) {
  return (
    <ul className="space-y-3">
      {resultados.map((item) => (
        <li key={item.cnpj}>
          <Link
            to={`/fornecedores/${encodeURIComponent(item.cnpj)}`}
            className="block focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-1 rounded-lg"
          >
            <Card className="hover:border-blue-300 hover:shadow-md transition-shadow">
              <div className="flex items-start justify-between gap-4">
                <div className="min-w-0 flex-1">
                  <p className="truncate font-medium text-gray-900">
                    {item.razao_social}
                  </p>
                  <div className="mt-1">
                    <CNPJFormatado cnpj={item.cnpj} link={false} />
                  </div>
                </div>
                <div className="flex flex-shrink-0 flex-col items-end gap-1.5">
                  <ScoreBadge valor={item.score_risco} faixa={item.faixa_risco} />
                  <ValorMonetario valor={item.valor_total} className="text-xs text-gray-500" />
                </div>
              </div>
            </Card>
          </Link>
        </li>
      ))}
    </ul>
  );
}
