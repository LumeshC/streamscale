import { act, renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { ApiError } from "../api/client";
import { DashboardProvider } from "./useDashboard";
import { usePolling } from "./usePolling";

const wrap = (refreshMs: number) =>
  function W({ children }: { children: ReactNode }) {
    return <DashboardProvider initialRefreshMs={refreshMs}>{children}</DashboardProvider>;
  };

afterEach(() => {
  vi.useRealTimers();
  Object.defineProperty(document, "hidden", { configurable: true, value: false });
});

describe("usePolling", () => {
  it("loads, then keeps stale data when a refresh fails", async () => {
    let n = 0;
    const fetcher = vi.fn(async () => {
      n++;
      if (n === 2) throw new ApiError("boom", 500, "internal");
      return n;
    });
    const { result } = renderHook(() => usePolling(fetcher, "k"), { wrapper: wrap(0) });
    expect(result.current.loading).toBe(true);
    await waitFor(() => expect(result.current.data).toBe(1));

    act(() => result.current.retry());
    await waitFor(() => expect(result.current.error?.code).toBe("internal"));
    expect(result.current.data).toBe(1); // stale data kept
    expect(result.current.loading).toBe(false);

    act(() => result.current.retry());
    await waitFor(() => expect(result.current.data).toBe(3));
    expect(result.current.error).toBeUndefined();
  });

  it("refetches on the dashboard interval and pauses while hidden", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const fetcher = vi.fn(async () => "x");
    renderHook(() => usePolling(fetcher, "k"), { wrapper: wrap(1000) });
    await waitFor(() => expect(fetcher).toHaveBeenCalledTimes(1));

    await act(async () => { vi.advanceTimersByTime(1000); });
    await waitFor(() => expect(fetcher).toHaveBeenCalledTimes(2));

    Object.defineProperty(document, "hidden", { configurable: true, value: true });
    act(() => { document.dispatchEvent(new Event("visibilitychange")); });
    await act(async () => { vi.advanceTimersByTime(5000); });
    expect(fetcher).toHaveBeenCalledTimes(2); // paused

    Object.defineProperty(document, "hidden", { configurable: true, value: false });
    act(() => { document.dispatchEvent(new Event("visibilitychange")); });
    await waitFor(() => expect(fetcher).toHaveBeenCalledTimes(3)); // immediate refresh on return
  });

  it("refetches when the key changes and aborts the superseded request", async () => {
    const signals: AbortSignal[] = [];
    const fetcher = vi.fn((s: AbortSignal) => {
      signals.push(s);
      return new Promise<string>(() => {}); // never resolves
    });
    const { rerender } = renderHook(({ k }) => usePolling(fetcher, k), { wrapper: wrap(0), initialProps: { k: "a" } });
    await waitFor(() => expect(fetcher).toHaveBeenCalledTimes(1));
    rerender({ k: "b" });
    await waitFor(() => expect(fetcher).toHaveBeenCalledTimes(2));
    expect(signals[0].aborted).toBe(true);
  });
});
