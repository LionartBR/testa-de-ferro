import { Button } from "./Button";

interface PaginationProps {
  page: number;
  hasMore: boolean;
  onPrev: () => void;
  onNext: () => void;
}

export function Pagination({ page, hasMore, onPrev, onNext }: PaginationProps) {
  return (
    <div className="flex items-center justify-between border-t border-gray-200 pt-4">
      <Button
        variant="secondary"
        size="sm"
        onClick={onPrev}
        disabled={page <= 1}
      >
        Anterior
      </Button>
      <span className="text-sm text-gray-500">
        P&aacute;gina {page}
      </span>
      <Button
        variant="secondary"
        size="sm"
        onClick={onNext}
        disabled={!hasMore}
      >
        Pr&oacute;xima
      </Button>
    </div>
  );
}
