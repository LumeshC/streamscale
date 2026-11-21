// Number/time formatting. Values from the API are already exact; these only
// present them.

const compactFmt = new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 });
const intFmt = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });

/** 1,284 below 10k; 12.9K / 4.2M above. */
export function formatCount(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return "—";
  return Math.abs(n) < 10_000 ? intFmt.format(n) : compactFmt.format(n);
}

/** Decimal-string amount (the API returns exact decimals as strings). */
export function formatAmount(v: string | number | null | undefined, compact = true): string {
  if (v == null || v === "") return "—";
  const n = typeof v === "number" ? v : Number(v);
  if (!Number.isFinite(n)) return "—";
  if (compact && Math.abs(n) >= 10_000) return compactFmt.format(n);
  return n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

/** 0.6777 → "67.8%" */
export function formatPercent(ratio: number | null | undefined, digits = 1): string {
  if (ratio == null || !Number.isFinite(ratio)) return "—";
  return `${(ratio * 100).toFixed(digits)}%`;
}

/** Rate per second/minute with sensible precision. */
export function formatRate(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return "—";
  if (Math.abs(n) >= 10_000) return compactFmt.format(n);
  return n.toLocaleString("en-US", { maximumFractionDigits: n < 10 ? 2 : 1 });
}

/** Milliseconds as "850 ms" / "1.24 s". */
export function formatMs(ms: number | null | undefined): string {
  if (ms == null || !Number.isFinite(ms)) return "—";
  if (ms < 1000) return `${ms.toFixed(ms < 10 ? 1 : 0)} ms`;
  return `${(ms / 1000).toFixed(2)} s`;
}

/** Axis/tooltip time label: HH:mm (or HH:mm:ss for second-level data). */
export function formatTime(iso: string, withSeconds = false): string {
  const d = new Date(iso);
  return d.toLocaleTimeString("en-GB", {
    hour: "2-digit",
    minute: "2-digit",
    ...(withSeconds ? { second: "2-digit" } : {}),
  });
}

export function formatDateTime(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleString("en-GB", {
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

/** "12s ago", "3m ago", "2h ago". */
export function formatAgo(from: Date | number | null, now: number = Date.now()): string {
  if (from == null) return "never";
  const t = typeof from === "number" ? from : from.getTime();
  const s = Math.max(0, Math.round((now - t) / 1000));
  if (s < 5) return "just now";
  if (s < 60) return `${s}s ago`;
  const m = Math.round(s / 60);
  if (m < 60) return `${m}m ago`;
  return `${Math.round(m / 60)}h ago`;
}

/** Human label for an API enum like PAYMENT_SUCCESS or invalid_json. */
export function humanize(key: string): string {
  const s = key.replace(/[_-]+/g, " ").toLowerCase();
  return s.charAt(0).toUpperCase() + s.slice(1);
}
