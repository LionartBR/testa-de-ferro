import { useState, useMemo } from "react";

interface PaginationState {
  page: number;
  limit: number;
  offset: number;
  setPage: (page: number) => void;
  nextPage: () => void;
  prevPage: () => void;
}

export function usePagination(limit = 20): PaginationState {
  const [page, setPage] = useState(1);

  return useMemo(
    () => ({
      page,
      limit,
      offset: (page - 1) * limit,
      setPage,
      nextPage: () => setPage((p) => p + 1),
      prevPage: () => setPage((p) => Math.max(1, p - 1)),
    }),
    [page, limit],
  );
}
