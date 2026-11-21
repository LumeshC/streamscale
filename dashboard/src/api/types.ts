// Response types for analytics-api. Mirrors analytics-api/api/openapi.yaml.

export interface Range {
  from: string;
  to: string;
}

export interface Page {
  limit: number;
  next_cursor?: string;
}

export interface ApiErrorBody {
  error: {
    code:
      | "invalid_parameter"
      | "not_found"
      | "method_not_allowed"
      | "timeout"
      | "unavailable"
      | "internal";
    message: string;
    details?: { field: string; reason: string }[];
  };
  request_id?: string;
}

export type CheckStatus = "ok" | "down" | "disabled";

export interface Check {
  status: CheckStatus;
  latency_ms?: number;
  error?: string;
}

export interface Health {
  status: "ok" | "degraded" | "unavailable";
  checks: { postgres?: Check; redis?: Check; kafka?: Check };
  time: string;
}

export interface Live {
  as_of: string;
  window_seconds: number;
  events: number;
  events_per_second: number;
  purchases: number;
  revenue: string;
  payment_success_rate: number;
  active_users: number;
  by_event_type: Record<string, number>;
  by_region: Record<string, number>;
}

export interface Overview {
  range: Range;
  totals: {
    events: number;
    events_per_minute: number;
    purchases: number;
    revenue: string;
    revenue_per_minute: string;
    payment_success: number;
    payment_failure: number;
    payment_success_rate: number;
    payment_failure_rate: number;
    peak_active_users_per_minute: number;
    windows: number;
  };
  coverage: { first_window_start: string | null; last_window_end: string | null };
  live: Live | null;
  degraded: string[];
}

export type Interval = "1s" | "1m" | "5m";

export interface Series<T, S = undefined> {
  range: Range;
  interval: Interval;
  summary?: S;
  data: T[];
  page: Page;
}

export interface EventPoint {
  window_start: string;
  window_end: string;
  events: number;
  events_per_second: number;
  events_per_minute: number;
  active_users: number;
}

export interface RevenuePoint {
  window_start: string;
  window_end: string;
  purchases: number;
  revenue: string;
  purchases_per_minute: number;
  revenue_per_minute: string;
}

export interface RevenueSummary {
  purchases: number;
  revenue: string;
  revenue_per_minute: string;
  average_order_value: string;
}

export interface PaymentPoint {
  window_start: string;
  window_end: string;
  success: number;
  failure: number;
  success_rate: number;
  failure_rate: number;
}

export interface PaymentSummary {
  success: number;
  failure: number;
  total: number;
  success_rate: number;
  failure_rate: number;
}

export interface Breakdown {
  range: Range;
  dimension: "region" | "event_type";
  total_events: number;
  data: { key: string; events: number; share: number }[];
}

export interface PartitionLag {
  partition: number;
  start_offset: number;
  end_offset: number;
  committed_offset: number;
  lag: number;
}

export interface ConsumerLag {
  group: string;
  topic: string;
  as_of: string;
  total_lag: number;
  end_offset_total: number;
  partitions: PartitionLag[];
}

export interface LatencyStats {
  count: number;
  p50_ms: number;
  p95_ms: number;
  p99_ms: number;
  max_ms: number;
}

export interface Latency {
  range: Range;
  definition: string;
  summary: LatencyStats;
  data: (LatencyStats & { minute: string })[];
}

export interface ErrorRecord {
  id: number;
  event_id: string | null;
  topic: string;
  partition: number;
  offset: number;
  failure_class: string;
  failure_reason: string;
  attempts: number;
  occurred_at: string;
}

export interface ErrorList {
  range: Range;
  totals: { processed: number; duplicate: number; dlq: number; error_rate: number };
  by_reason: { failure_class: string; failure_reason: string; count: number }[];
  data: ErrorRecord[];
  page: Page;
}
