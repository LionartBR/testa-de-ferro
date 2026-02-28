import { ApiError } from "@/types/api";

const BASE_URL = "/api";

export async function apiFetch<T>(
  path: string,
  init?: RequestInit,
): Promise<T> {
  const url = `${BASE_URL}${path}`;
  const response = await fetch(url, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...init?.headers,
    },
  });

  if (!response.ok) {
    const body = await response.json().catch(() => ({
      detail: response.statusText,
    }));
    throw new ApiError(
      response.status,
      (body as { detail?: string }).detail ?? response.statusText,
    );
  }

  return response.json() as Promise<T>;
}

export async function apiFetchBlob(
  path: string,
  init?: RequestInit,
): Promise<Blob> {
  const url = `${BASE_URL}${path}`;
  const response = await fetch(url, init);

  if (!response.ok) {
    const body = await response.json().catch(() => ({
      detail: response.statusText,
    }));
    throw new ApiError(
      response.status,
      (body as { detail?: string }).detail ?? response.statusText,
    );
  }

  return response.blob();
}
