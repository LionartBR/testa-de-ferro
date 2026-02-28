import { formatCurrency } from "@/lib/formatters";

interface ValorMonetarioProps {
  valor: string | number;
  className?: string;
}

export function ValorMonetario({ valor, className = "" }: ValorMonetarioProps) {
  return (
    <span className={`tabular-nums ${className}`}>
      {formatCurrency(valor)}
    </span>
  );
}
