import { render, screen, waitFor, within } from "@testing-library/react";
import { Dashboard } from "./App";
import { DashboardProvider } from "./hooks/useDashboard";
import { lagTone } from "./sections/ConsumerLag";
import { mockFetch } from "./test-utils";

const now = Date.parse("2026-11-21T00:40:00Z");
const win = (i: number) => new Date(now - (3 - i) * 60_000).toISOString();
const range = { from: win(0), to: win(3) };

function fixtures(url: URL) {
  const p = url.pathname.replace(/^\/api/, "");
  switch (p) {
    case "/health":
      return { body: { status: "degraded", time: win(3), checks: { postgres: { status: "ok" }, redis: { status: "ok" }, kafka: { status: "down", error: "dial tcp" } } } };
    case "/metrics/overview":
      return { body: { range, totals: { events: 12900, events_per_minute: 4300, purchases: 10, revenue: "1234.50", revenue_per_minute: "411.50", payment_success: 9, payment_failure: 1, payment_success_rate: 0.9, payment_failure_rate: 0.1, peak_active_users_per_minute: 250, windows: 3 }, coverage: { first_window_start: win(0), last_window_end: win(3) }, live: null, degraded: ["live"] } };
    case "/metrics/events":
      return { body: { range, interval: "1m", data: [0, 1, 2].map((i) => ({ window_start: win(i), window_end: win(i + 1), events: 4000 + i, events_per_second: 66.7, events_per_minute: 4000, active_users: 240 + i })), page: { limit: 1000 } } };
    case "/metrics/revenue":
      return { body: { range, interval: "1m", summary: { purchases: 10, revenue: "1234.50", revenue_per_minute: "411.50", average_order_value: "123.45" }, data: [], page: { limit: 1000 } } };
    case "/metrics/payments":
      return { body: { range, interval: "1m", summary: { success: 9, failure: 1, total: 10, success_rate: 0.9, failure_rate: 0.1 }, data: [{ window_start: win(0), window_end: win(1), success: 9, failure: 1, success_rate: 0.9, failure_rate: 0.1 }], page: { limit: 1000 } } };
    case "/metrics/regions":
      return { body: { range, dimension: "region", total_events: 100, data: [{ key: "us-east-1", events: 60, share: 0.6 }, { key: "eu-west-1", events: 40, share: 0.4 }] } };
    case "/metrics/event-types":
      return { body: { range, dimension: "event_type", total_events: 100, data: [{ key: "PAGE_VIEW", events: 100, share: 1 }] } };
    case "/metrics/consumer-lag":
      return { status: 503, body: { error: { code: "unavailable", message: "kafka is unavailable" }, request_id: "rid-lag" } };
    case "/metrics/latency":
      return { body: { range, definition: "x", summary: { count: 100, p50_ms: 12, p95_ms: 40, p99_ms: 80, max_ms: 120 }, data: [] } };
    case "/errors":
      return { body: { range, totals: { processed: 99, duplicate: 0, dlq: 1, error_rate: 0.01 }, by_reason: [{ failure_class: "malformed", failure_reason: "invalid_json", count: 1 }], data: [{ id: 1, event_id: null, topic: "events", partition: 2, offset: 7, failure_class: "malformed", failure_reason: "invalid_json", attempts: 0, occurred_at: win(2) }], page: { limit: 25 } } };
  }
  return undefined;
}

afterEach(() => vi.unstubAllGlobals());

describe("Dashboard", () => {
  it("renders all ten sections from the API, including per-section failures", async () => {
    const { calls } = mockFetch(fixtures);
    render(
      <DashboardProvider initialRefreshMs={0}>
        <Dashboard />
      </DashboardProvider>,
    );

    for (const title of [
      "System overview", "Event throughput", "Kafka consumer lag", "Processing latency", "Active users",
      "Revenue", "Payment success / failure", "Regional distribution", "Event type distribution", "Recent errors",
    ]) {
      expect(screen.getByRole("heading", { name: title })).toBeInTheDocument();
    }

    // Data arrives.
    await waitFor(() => expect(screen.getByText("12.9K")).toBeInTheDocument());
    expect(screen.getByText("Kafka down")).toBeInTheDocument();
    expect(screen.getByText("System degraded")).toBeInTheDocument();
    expect(screen.getByText(/Live view degraded/)).toBeInTheDocument();
    expect(await screen.findByText("us-east-1")).toBeInTheDocument();
    expect(await screen.findByText("12 ms")).toBeInTheDocument();
    expect(await screen.findByText("Invalid json")).toBeInTheDocument();

    // One section failing does not take down the others.
    const lag = document.getElementById("lag")!;
    await waitFor(() => expect(within(lag).getByRole("alert")).toHaveTextContent("A backend dependency is unavailable."));
    expect(within(lag).getByText(/rid-lag/)).toBeInTheDocument();

    // Only documented query parameters are sent (the API rejects unknown ones;
    // allowed sets mirror analytics-api/api/openapi.yaml).
    const allowed: Record<string, string[]> = {
      "/api/health": [],
      "/api/metrics/overview": ["from", "to", "live_window"],
      "/api/metrics/events": ["from", "to", "interval", "limit", "cursor"],
      "/api/metrics/revenue": ["from", "to", "interval", "limit", "cursor"],
      "/api/metrics/payments": ["from", "to", "interval", "limit", "cursor"],
      "/api/metrics/regions": ["from", "to"],
      "/api/metrics/event-types": ["from", "to", "event_type"],
      "/api/metrics/consumer-lag": [],
      "/api/metrics/latency": ["from", "to"],
      "/api/errors": ["from", "to", "class", "limit", "cursor"],
    };
    for (const u of calls) {
      const ok = allowed[u.pathname];
      expect(ok, `unexpected path ${u.pathname}`).toBeDefined();
      for (const k of u.searchParams.keys()) expect(ok, `${u.pathname} sent ?${k}`).toContain(k);
    }

    // Only analytics-api paths are called, all through /api.
    expect(calls.every((u) => u.pathname.startsWith("/api/"))).toBe(true);
    const ev = calls.find((u) => u.pathname === "/api/metrics/events")!;
    expect(ev.searchParams.get("interval")).toBe("1m");
    expect(ev.searchParams.get("from")).toBeTruthy();
  });

  it("lag thresholds", () => {
    expect(lagTone(0).tone).toBe("good");
    expect(lagTone(5_000).tone).toBe("warning");
    expect(lagTone(50_000).tone).toBe("critical");
  });
});
