import { Link } from "react-router-dom";
import { formatCNPJ } from "@/lib/formatters";

interface CNPJFormatadoProps {
  cnpj: string;
  link?: boolean;
}

export function CNPJFormatado({ cnpj, link = true }: CNPJFormatadoProps) {
  const formatted = formatCNPJ(cnpj);

  if (link) {
    return (
      <Link
        to={`/fornecedores/${encodeURIComponent(cnpj)}`}
        className="font-mono text-blue-600 hover:underline"
      >
        {formatted}
      </Link>
    );
  }

  return <span className="font-mono">{formatted}</span>;
}
