import { getAllPages, getJSON } from "./client";
import type {
  Breakdown,
  ConsumerLag,
  ErrorList,
  EventPoint,
  Health,
  Interval,
  Latency,
  Overview,
  PaymentPoint,
  PaymentSummary,
  RevenuePoint,
  RevenueSummary,
  Series,
} from "./types";

// Typed calls to analytics-api. Series are returned oldest → newest with an
// epoch-ms `t`, ready for charts.

export type TimeWindow = { from: string; to: string };

/** Only from/to go on the wire: the API rejects unknown parameters. */
const rng = (w: TimeWindow) => ({ from: w.from, to: w.to });

export const api = {
  health: (signal: AbortSignal) => getJSON<Health>("/health", {}, signal),

  overview: (w: TimeWindow, signal: AbortSignal) =>
    getJSON<Overview>("/metrics/overview", { ...rng(w), live_window: 60 }, signal),

  events: async (w: TimeWindow, interval: Interval, signal: AbortSignal) => {
    const { data, truncated } = await getAllPages<EventPoint, Series<EventPoint>>(
      "/metrics/events",
      { ...rng(w), interval, limit: 1000 },
      signal,
    );
    return { points: chronological(data), truncated };
  },

  revenue: async (w: TimeWindow, signal: AbortSignal) => {
    const { first, data, truncated } = await getAllPages<RevenuePoint, Series<RevenuePoint, RevenueSummary>>(
      "/metrics/revenue",
      { ...rng(w), interval: "1m", limit: 1000 },
      signal,
    );
    return { summary: first.summary!, points: chronological(data), truncated };
  },

  payments: async (w: TimeWindow, signal: AbortSignal) => {
    const { first, data, truncated } = await getAllPages<PaymentPoint, Series<PaymentPoint, PaymentSummary>>(
      "/metrics/payments",
      { ...rng(w), interval: "1m", limit: 1000 },
      signal,
    );
    return { summary: first.summary!, points: chronological(data), truncated };
  },

  regions: (w: TimeWindow, signal: AbortSignal) => getJSON<Breakdown>("/metrics/regions", rng(w), signal),

  eventTypes: (w: TimeWindow, signal: AbortSignal) => getJSON<Breakdown>("/metrics/event-types", rng(w), signal),

  consumerLag: (signal: AbortSignal) => getJSON<ConsumerLag>("/metrics/consumer-lag", {}, signal),

  latency: (w: TimeWindow, signal: AbortSignal) => getJSON<Latency>("/metrics/latency", rng(w), signal),

  errors: (w: TimeWindow, cls: string, signal: AbortSignal) =>
    getJSON<ErrorList>("/errors", { ...rng(w), class: cls === "all" ? undefined : cls, limit: 25 }, signal),
};

/** API pages are newest first; charts want oldest first with numeric time. */
export function chronological<T extends { window_start: string }>(rows: T[]): (T & { t: number })[] {
  return rows
    .map((r) => ({ ...r, t: Date.parse(r.window_start) }))
    .sort((a, b) => a.t - b.t);
}
