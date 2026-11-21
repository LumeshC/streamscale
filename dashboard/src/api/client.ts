import type { ApiErrorBody, Page } from "./types";

// All requests go through /api, which the dev server (Vite) and the production
// image (nginx) proxy to analytics-api. Override with VITE_API_BASE if needed.
export const API_BASE: string = import.meta.env.VITE_API_BASE ?? "/api";

/** Client-side cap per request; the API's own budget is 5 s. */
export const REQUEST_TIMEOUT_MS = 8000;

/** An analytics-api error (or a network/timeout failure) in one shape. */
export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number, // 0 = network error / timeout
    readonly code: string,
    readonly requestId?: string,
    readonly details?: { field: string; reason: string }[],
  ) {
    super(message);
    this.name = "ApiError";
  }

  /** Short, user-facing explanation. */
  get userMessage(): string {
    switch (this.code) {
      case "unavailable":
        return "A backend dependency is unavailable.";
      case "timeout":
        return "The request took too long.";
      case "network":
        return "Cannot reach the analytics API.";
      case "invalid_parameter":
        return "The request was rejected as invalid.";
      default:
        return "Something went wrong loading this data.";
    }
  }
}

export type Query = Record<string, string | number | undefined>;

export function buildUrl(path: string, query: Query = {}): string {
  const params = new URLSearchParams();
  for (const [k, v] of Object.entries(query)) {
    if (v !== undefined && v !== "") params.set(k, String(v));
  }
  const qs = params.toString();
  return `${API_BASE}${path}${qs ? `?${qs}` : ""}`;
}

/**
 * GET a JSON resource from analytics-api. Combines the caller's abort signal
 * (unmount / superseded refresh) with a timeout, and turns every failure into
 * an ApiError carrying the API's error envelope when there is one.
 */
export async function getJSON<T>(path: string, query: Query = {}, signal?: AbortSignal): Promise<T> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(new DOMException("timeout", "TimeoutError")), REQUEST_TIMEOUT_MS);
  const onAbort = () => ctrl.abort(signal?.reason);
  signal?.addEventListener("abort", onAbort);
  try {
    let res: Response;
    try {
      res = await fetch(buildUrl(path, query), { signal: ctrl.signal, headers: { Accept: "application/json" } });
    } catch (e) {
      if (signal?.aborted) throw e; // caller cancelled: propagate AbortError untouched
      const timedOut = ctrl.signal.reason instanceof DOMException && ctrl.signal.reason.name === "TimeoutError";
      throw timedOut
        ? new ApiError("Request timed out", 0, "timeout")
        : new ApiError("Network error", 0, "network");
    }
    const requestId = res.headers.get("X-Request-ID") ?? undefined;
    let body: unknown = null;
    try {
      body = await res.json();
    } catch {
      /* non-JSON body (e.g. proxy error page) */
    }
    if (!res.ok) {
      const env = body as ApiErrorBody | null;
      if (env?.error) {
        throw new ApiError(env.error.message, res.status, env.error.code, env.request_id ?? requestId, env.error.details);
      }
      // 502/504 from the proxy when analytics-api itself is down.
      const code = res.status === 502 || res.status === 503 || res.status === 504 ? "network" : "internal";
      throw new ApiError(`HTTP ${res.status}`, res.status, code, requestId);
    }
    return body as T;
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener("abort", onAbort);
  }
}

/**
 * Follow keyset pagination (page.next_cursor) and concatenate `data`.
 * `maxPages` bounds the work for very long ranges.
 */
export async function getAllPages<T, R extends { data: T[]; page: Page }>(
  path: string,
  query: Query,
  signal?: AbortSignal,
  maxPages = 5,
): Promise<{ first: R; data: T[]; truncated: boolean }> {
  const first = await getJSON<R>(path, query, signal);
  const data = [...first.data];
  let cursor = first.page.next_cursor;
  let pages = 1;
  while (cursor && pages < maxPages) {
    const next = await getJSON<R>(path, { ...query, cursor }, signal);
    data.push(...next.data);
    cursor = next.page.next_cursor;
    pages++;
  }
  return { first, data, truncated: Boolean(cursor) };
}
