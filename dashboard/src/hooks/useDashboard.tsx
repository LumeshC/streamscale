import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from "react";

// Dashboard-wide controls: one time range and one refresh cadence scope every
// section, so all numbers on screen describe the same slice of time.

export interface RangePreset {
  id: string;
  label: string;
  minutes: number;
}

export const RANGE_PRESETS: RangePreset[] = [
  { id: "15m", label: "Last 15 min", minutes: 15 },
  { id: "1h", label: "Last hour", minutes: 60 },
  { id: "6h", label: "Last 6 hours", minutes: 360 },
  { id: "24h", label: "Last 24 hours", minutes: 1440 },
];

export const REFRESH_OPTIONS = [
  { ms: 5_000, label: "5s" },
  { ms: 10_000, label: "10s" },
  { ms: 30_000, label: "30s" },
  { ms: 60_000, label: "1m" },
  { ms: 0, label: "Off" },
];

interface DashboardState {
  range: RangePreset;
  setRangeId: (id: string) => void;
  refreshMs: number;
  setRefreshMs: (ms: number) => void;
  /** Increments on "Refresh now"; polling hooks refetch when it changes. */
  tick: number;
  refreshNow: () => void;
  /** Resolves the current [from, to) at call time (ISO strings). */
  window: (capMinutes?: number) => { from: string; to: string; minutes: number };
}

const Ctx = createContext<DashboardState | null>(null);

function readStored<T>(key: string, fallback: T): T {
  try {
    const v = localStorage.getItem(key);
    return v == null ? fallback : (JSON.parse(v) as T);
  } catch {
    return fallback;
  }
}

function store(key: string, v: unknown) {
  try {
    localStorage.setItem(key, JSON.stringify(v));
  } catch {
    /* storage unavailable: preferences just won't persist */
  }
}

export function DashboardProvider({ children, initialRefreshMs }: { children: ReactNode; initialRefreshMs?: number }) {
  const [rangeId, setRangeIdState] = useState(() => readStored("ss.range", "1h"));
  const [refreshMs, setRefreshMsState] = useState(() => initialRefreshMs ?? readStored("ss.refresh", 10_000));
  const [tick, setTick] = useState(0);

  const range = RANGE_PRESETS.find((p) => p.id === rangeId) ?? RANGE_PRESETS[1];
  const setRangeId = useCallback((id: string) => {
    setRangeIdState(id);
    store("ss.range", id);
  }, []);
  const setRefreshMs = useCallback((ms: number) => {
    setRefreshMsState(ms);
    store("ss.refresh", ms);
  }, []);
  const refreshNow = useCallback(() => setTick((t) => t + 1), []);

  const value = useMemo<DashboardState>(
    () => ({
      range,
      setRangeId,
      refreshMs,
      setRefreshMs,
      tick,
      refreshNow,
      window: (capMinutes?: number) => {
        const minutes = capMinutes ? Math.min(range.minutes, capMinutes) : range.minutes;
        const to = new Date();
        const from = new Date(to.getTime() - minutes * 60_000);
        return { from: from.toISOString(), to: to.toISOString(), minutes };
      },
    }),
    [range, setRangeId, refreshMs, setRefreshMs, tick, refreshNow],
  );
  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useDashboard(): DashboardState {
  const v = useContext(Ctx);
  if (!v) throw new Error("useDashboard must be used inside <DashboardProvider>");
  return v;
}

/** Re-render every `ms` (for "updated 12s ago" labels). */
export function useNow(ms = 1000): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), ms);
    return () => clearInterval(id);
  }, [ms]);
  return now;
}
