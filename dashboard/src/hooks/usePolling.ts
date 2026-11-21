import { useCallback, useEffect, useRef, useState } from "react";
import { ApiError } from "../api/client";
import { useDashboard } from "./useDashboard";

export interface PollState<T> {
  data: T | undefined;
  error: ApiError | undefined;
  /** First load, nothing to show yet. */
  loading: boolean;
  /** A refetch is in flight while previous data stays on screen. */
  refreshing: boolean;
  updatedAt: number | null;
  retry: () => void;
}

/**
 * Fetches on mount, whenever `key` changes (e.g. time range), on the dashboard's
 * refresh cadence, on "Refresh now", and when the tab becomes visible again.
 *
 * - Keeps the last good data while refetching or after an error (stale-while-
 *   revalidate), so charts hold their frame instead of flashing a skeleton.
 * - Aborts the in-flight request when superseded or on unmount.
 * - Pauses while the tab is hidden.
 */
export function usePolling<T>(fetcher: (signal: AbortSignal) => Promise<T>, key: string): PollState<T> {
  const { refreshMs, tick } = useDashboard();
  const [data, setData] = useState<T>();
  const [error, setError] = useState<ApiError>();
  const [inFlight, setInFlight] = useState(false);
  const [updatedAt, setUpdatedAt] = useState<number | null>(null);
  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;
  const ctrlRef = useRef<AbortController | null>(null);

  const run = useCallback(async () => {
    ctrlRef.current?.abort();
    const ctrl = new AbortController();
    ctrlRef.current = ctrl;
    setInFlight(true);
    try {
      const d = await fetcherRef.current(ctrl.signal);
      if (ctrl.signal.aborted) return;
      setData(d);
      setError(undefined);
      setUpdatedAt(Date.now());
    } catch (e) {
      if (ctrl.signal.aborted) return;
      setError(e instanceof ApiError ? e : new ApiError(String(e), 0, "internal"));
    } finally {
      if (ctrlRef.current === ctrl) setInFlight(false);
    }
  }, []);

  // Initial load + key changes + manual refresh.
  useEffect(() => {
    void run();
  }, [run, key, tick]);

  // Interval refresh, paused while hidden; refresh on becoming visible.
  useEffect(() => {
    if (!refreshMs) return;
    let id: ReturnType<typeof setInterval> | undefined;
    const start = () => {
      if (id === undefined) id = setInterval(() => void run(), refreshMs);
    };
    const stop = () => {
      if (id !== undefined) clearInterval(id);
      id = undefined;
    };
    const onVis = () => {
      if (document.hidden) stop();
      else {
        void run();
        start();
      }
    };
    if (!document.hidden) start();
    document.addEventListener("visibilitychange", onVis);
    return () => {
      stop();
      document.removeEventListener("visibilitychange", onVis);
    };
  }, [run, refreshMs]);

  useEffect(() => () => ctrlRef.current?.abort(), []);

  return {
    data,
    error,
    loading: data === undefined && error === undefined,
    refreshing: inFlight && data !== undefined,
    updatedAt,
    retry: () => void run(),
  };
}
