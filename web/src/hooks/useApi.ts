import { useState, useEffect, useCallback } from "react";
import { ApiError } from "@/types/api";

interface IdleState {
  status: "idle";
  data: undefined;
  error: undefined;
}

interface LoadingState {
  status: "loading";
  data: undefined;
  error: undefined;
}

interface SuccessState<T> {
  status: "success";
  data: T;
  error: undefined;
}

interface ErrorState {
  status: "error";
  data: undefined;
  error: ApiError;
}

export type ApiState<T> =
  | IdleState
  | LoadingState
  | SuccessState<T>
  | ErrorState;

export function useApi<T>(
  fetcher: (() => Promise<T>) | null,
): ApiState<T> & { refetch: () => void } {
  const [state, setState] = useState<ApiState<T>>({
    status: fetcher ? "loading" : "idle",
    data: undefined,
    error: undefined,
  });

  const execute = useCallback(() => {
    if (!fetcher) return;

    setState({ status: "loading", data: undefined, error: undefined });

    fetcher().then(
      (data) => {
        setState({ status: "success", data, error: undefined });
      },
      (err: unknown) => {
        const apiError =
          err instanceof ApiError
            ? err
            : new ApiError(0, err instanceof Error ? err.message : "Erro desconhecido");
        setState({ status: "error", data: undefined, error: apiError });
      },
    );
  }, [fetcher]);

  useEffect(() => {
    execute();
  }, [execute]);

  return { ...state, refetch: execute };
}
